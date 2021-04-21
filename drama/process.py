import json
import tempfile
from abc import abstractmethod
from datetime import datetime
from enum import Enum
from typing import Dict, Iterator, List, Optional, Tuple, Union

from kafka import KafkaConsumer, KafkaProducer

from drama.config import settings
from drama.datatype import DataType, get_dict, get_schema
from drama.logger import get_logger
from drama.models.messages import Message, MessageType, Servo, SignalMessage, SignalType
from drama.servo import deserialize, serialize
from drama.storage.base import Resource, Storage
from drama.storage.helpers import get_available_storage


class LoggingMessageType(str, Enum):
    INFO = "INFO"
    DEBUG = "DEBUG"
    WARN = "WARNING"
    ERROR = "ERROR"


class BaseProcess:
    """
    Process class inherited by user process script.
    Its attributes and methods can be accessed from within an `execute` Python function.
    """

    def __init__(
        self,
        name: str,
        module: str,
        parent: str,
        params: dict,
        inputs: Optional[Dict[str, str]] = None,
        storage: Optional[Storage] = None,
    ):
        """
        :param name: Task name.
        :param module: Task module.
        :param parent: Parent task id.
        """
        self.name = name
        self.module = module
        self.parent = parent
        self.params = params

        # inputs
        if not inputs:
            inputs = {}
        self.inputs = inputs

        # storage
        if not storage:
            _storage = get_available_storage()
            storage = _storage(bucket_name=self.parent, folder_name=self.name)
        self.storage = storage
        self.storage.setup()

        # logging
        self.logger = get_logger(__name__)
        self.logging_file = tempfile.NamedTemporaryFile(dir=storage.temp_dir)

    @abstractmethod
    def to_downstream(self, data: DataType) -> Message:
        pass

    @abstractmethod
    def poll_from_upstream(self, apply_servo: bool = True) -> Iterator[Tuple[str, dict]]:
        pass

    @abstractmethod
    def get_from_upstream(self, **kwargs) -> Dict[str, List[dict]]:
        pass

    @abstractmethod
    def close(self, force_interruption: bool = False, destroy_storage: bool = False) -> Resource:
        pass

    def info(self, message: Union[str, List[str]]) -> None:
        """
        Logs INFO message.
        """
        self.logger.info(message)
        self._log(message, LoggingMessageType.INFO)

    def debug(self, message: Union[str, List[str]]) -> None:
        """
        Logs DEBUG message.
        """
        self.logger.debug(message)
        self._log(message, LoggingMessageType.DEBUG)

    def warn(self, message: Union[str, List[str]]) -> None:
        """
        Logs WARNING message.
        """
        self.logger.warn(message)
        self._log(message, LoggingMessageType.WARN)

    def error(self, message: Union[str, List[str]]) -> None:
        """
        Logs ERROR message.
        """
        self.logger.exception(message)
        self._log(message, LoggingMessageType.ERROR)

    def _log(self, message: Union[str, List[str]], flag: LoggingMessageType) -> None:
        """
        Writes message to logging file.
        """
        if not isinstance(message, list):
            message = [message]
        with open(self.logging_file.name, "a+") as log:
            log.write(f"[{flag}] [{datetime.now()}] {message.pop(0)}\n")
            for line in message:
                log.write(f"{line}\n")


class Process(BaseProcess):
    """
    Actor process based on Apache Kafka and Avro for inter-component communication.
    """

    MESSAGE_SCHEMA = {
        "type": "record",
        "name": "message",
        "namespace": "drama.process",
        "fields": [
            {"name": "type", "type": "string"},
            {"name": "key", "type": "string", "default": "undefined"},
            {"name": "data", "type": "bytes"},
            {"name": "servo", "type": "string", "default": "undefined"},
            {"name": "schem", "type": "string", "default": "undefined"},
        ],
    }

    def __init__(
        self,
        name: str,
        module: str,
        parent: str,
        params: dict,
        inputs: Optional[Dict[str, str]] = None,
        storage: Optional[Storage] = None,
    ):
        super().__init__(name, module, parent, params, inputs, storage)
        self.logger = get_logger(__name__, name=name)

    def to_downstream(self, data: DataType) -> Message:
        """
        Sends block-like message thought Kafka topic.
        Data is serialized using self-contained schema.
        """
        servo_schema = get_schema(data)
        dict_data = get_dict(data)

        # serialize data
        serialized_data = serialize(dict_data, servo_schema)
        stringify_schema = json.dumps(servo_schema, default=str)

        # wrapper
        message_key = f"{self.name}.{data.key}"
        message = Message(
            type=MessageType.BLOCK,
            key=message_key,
            data=serialized_data,
            schem=stringify_schema,
            servo=Servo.AVRO,
        )

        self.debug([f"Sending {message_key} to downstream"])
        self._send(message)

        return message

    def poll_from_upstream(self, apply_servo: bool = True) -> Iterator[Tuple[str, dict]]:
        """
        Polls messages from input task(s).

        Only records from input task(s) are handled (i.e., records with record key not in input task names
        are ignored). Records with incoming task name equals to parent id are also handled.

        Input records are deserialized using self-process schema and processed according to the
        message type.

        :param apply_servo: If true, incoming messages are automatically deserialized. Defaults to `True`.
        """
        if not self.inputs:
            raise Exception("Tried to poll from upstream, but no input defined")

        inputs_names = [inn.split(".")[0] for inn in self.inputs.values()]  # eg., [Task1, Task2]
        inputs_remaining = [v for _, v in self.inputs.items()]  # eg., [Task1.Data1, Task1.Data2, Task2.Data1]
        inputs_reversed = {v: k for k, v in self.inputs.items()}

        number_of_input_tasks = len(set(inputs_names))

        self.debug(
            f"Declared input tasks ({number_of_input_tasks}): {inputs_names}, expected inputs: {inputs_remaining}",
        )

        counter = 0
        consumer = self._consumer()

        while counter < number_of_input_tasks:
            messages = consumer.poll(timeout_ms=1)
            for _, msg in messages.items():
                for record in msg:
                    # only read incoming messages from input tasks, ignore the rest
                    incoming_task_name = record.key.decode("utf-8")

                    if incoming_task_name not in inputs_names and incoming_task_name != self.parent:
                        continue  # go back to the beginning of the inner loop

                    self.debug([f"Got message from {incoming_task_name}"])

                    # deserialize message
                    serialized_message = record.value
                    deserialized_message: Dict[Message] = deserialize(serialized_message, self.MESSAGE_SCHEMA)  # type: ignore

                    message = Message(**deserialized_message)

                    # check message type
                    if message.type == MessageType.SIGNAL:
                        if message.data == SignalType.INTE:
                            self.warn([f"Received interruption signal from task {incoming_task_name}"])
                            raise Exception(f"Task was brutally murdered by upstream with signal {SignalType.INTE}")
                        elif message.data == SignalType.STOP:
                            self.debug([f"Received {SignalType.STOP} signal from task {incoming_task_name}"])
                            counter += 1  # stops the while loop when counter equals number_of_input_tasks
                        else:
                            raise NotImplementedError(f"Unrecognized signal {message.data}")
                    elif message.type == MessageType.BLOCK:
                        # input tasks can send multiple messages with different data attached (eg., DataA and DataB),
                        #  but we might be only interested in some of them
                        message_key: str = message.key

                        self.debug([f"Received {message_key} from task {incoming_task_name}"])

                        if message_key not in self.inputs.values():
                            # received message is not an input of this task
                            self.debug([f"Discarding message {message_key}"])
                            continue

                        try:
                            # delete from remaining list
                            inputs_remaining.remove(message_key)
                        except ValueError:
                            pass

                        datatype = message.data

                        if apply_servo:
                            serialized_datatype: bytes = message.data
                            schema: str = message.schem
                            datatype = deserialize(serialized_datatype, json.loads(schema))

                        self.debug([f"{incoming_task_name} content: {datatype}"])

                        # returns key and data
                        yield inputs_reversed[message_key], datatype
                    else:
                        raise NotImplementedError(f"Unrecognized message type: {message.type}")

        # at this point, all tasks have send STOP signals
        # check if all inputs have been yielded (as expected)
        if len(inputs_remaining) > 0:
            raise Exception(f"Some inputs were declared but are missing: {inputs_remaining}")

        consumer.close()

    def get_from_upstream(self, **kwargs) -> Dict[str, List[dict]]:
        """
        Waits for all messages from input task(s) and returns dictionary with results.
        """
        messages: Dict[str, list] = {}
        polling = self.poll_from_upstream(**kwargs)

        for message_key, message in list(polling):
            messages.setdefault(message_key, []).append(message)

        return messages

    def close(self, force_interruption: bool = False, remove_local_dir: bool = False) -> Resource:
        """
        Sends close message thought Kafka topic, to report that task has ended and will no longer
        produce messages.

        If `force_interruption` is set to True, send interruption message instead (*note*: this flag will
        trigger an exception chain against downstream tasks in a workflow).
        """
        if force_interruption:
            self.error(["Task brutally interrupted"])
        else:
            self.debug(["Task gracefully closed"])

        logging_filename = "log.txt"

        # send log to remote storage
        logging_remote = self.storage.put_file(self.logging_file.name, rename=logging_filename)

        # once uploaded, delete named temporal file in local temp dir
        self.logging_file.close()

        if remove_local_dir:
            # remove local directory without deleting the logging file (always kept for debugging purposes)
            self.storage.remove_local_dir(omit_files=[logging_filename])

        # send interruption signal
        signal = SignalMessage(data=SignalType.INTE if force_interruption else SignalType.STOP)
        self._send(signal)

        return logging_remote

    def _send(self, message: Union[Message, SignalMessage], **kwargs) -> None:
        """
        Sends message thought topic.

        :returns: Serialized input message.
        """
        serialized_message = serialize(message.dict(), self.MESSAGE_SCHEMA)

        # send thought topic
        producer = self._producer()
        producer.send(topic=self.parent, value=serialized_message, key=bytes(self.name, "utf-8"), **kwargs)
        producer.close()

    def _producer(self, **kwargs) -> KafkaProducer:
        """
        Creates a new Kafka producer.
        """
        return KafkaProducer(bootstrap_servers=[settings.KAFKA_CONN], **kwargs)

    def _consumer(self, **kwargs) -> KafkaConsumer:
        """
        Creates a new Kafka consumer.
        """
        return KafkaConsumer(
            self.parent,
            bootstrap_servers=[settings.KAFKA_CONN],
            auto_offset_reset="earliest",
            **kwargs,
        )
