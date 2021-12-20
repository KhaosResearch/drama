from drama.config import settings
from drama.core.model import DynamicParameter
from drama.models.task import TaskResult
from drama.process import Process


def execute(pcs: Process, **kwargs) -> None:
    """
    Gets a message from own Kafka topic.
    """
    topic = f"{pcs.parent}-{pcs.name}"
    value = None

    consumer = pcs._consumer(topic)

    messages = consumer.poll(max_records=1, timeout_ms=600000)
    for _, msg in messages.items():
        for record in msg:
            value = record.value.decode("utf-8")

    if not value:
        raise Exception(f"No value found in topic {topic} after 600000ms")

    # send to downstream
    output_temp_file = DynamicParameter(value=value)
    pcs.to_downstream(output_temp_file)

    return TaskResult(message=value)
