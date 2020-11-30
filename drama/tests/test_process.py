import collections
import unittest
from dataclasses import dataclass
from unittest import mock
from unittest.mock import MagicMock

from drama.datatype import DataType, is_integer
from drama.models.messages import MessageType, Servo
from drama.process import Process


@dataclass
class Point(DataType):
    x: int = is_integer()
    y: int = is_integer()


@dataclass
class PointA(Point):
    pass


@dataclass
class PointB(Point):
    pass


class ProcessWithOneInputOneUpstreamTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.process = Process(
            name="test-task-1",
            module="",
            params={},
            parent="test-workflow-1",
            inputs={"point": "test-task-0.Point"},
        )

    @mock.patch("drama.process.Process._producer")
    def test_should_send_point_to_downstream(self, producer):
        producer.return_value = MagicMock()  # mock self._producer() to avoid kafka producer

        point = Point(1, 2)

        message = self.process.to_downstream(data=point)

        self.assertEqual(MessageType.BLOCK, message.type)
        self.assertEqual("test-task-1.Point", message.key)
        self.assertEqual(b"\x02\x04", message.data)
        self.assertEqual(
            '{"namespace": "tests.test_process", "name": "Point", "type": "record", "fields": [{"name": "x", "type": "int"}, {"name": "y", "type": "int"}]}',
            message.schem,
        )
        self.assertEqual(Servo.AVRO, message.servo)

    @mock.patch("drama.process.Process._consumer")
    def test_should_poll_messages_from_upstream(self, consumer):
        def poll(**kwargs):
            _TopicPartition = collections.namedtuple("TopicPartition", [])
            _ConsumerRecord = collections.namedtuple("ConsumerRecord", ["key", "value"])

            point_record = _ConsumerRecord(
                key=b"test-task-0",
                value=b'\nBLOCK"test-task-0.Point\x04\x02\x04\x08AVRO\xd8\x03{"namespace": "drama.examples.publisher.DemoSinglePublisherPoint", "name": "Point", "type": "record", "fields": [{"name": "x", "type": "int"}, {"name": "y", "type": "int"}]}',
            )

            stop_record = _ConsumerRecord(
                key=b"test-task-0",
                value=b"\x0cSIGNAL\x12undefined\x18POISSON_PILL\x12undefined\x12undefined",
            )

            return {_TopicPartition: [point_record, stop_record]}

        mocked_consumer = MagicMock()
        mocked_consumer.poll = poll

        consumer.return_value = mocked_consumer  # mock self._consumer() to avoid kafka producer

        for key, msg in self.process.poll_from_upstream():
            self.assertEqual("point", key)
            self.assertEqual({"x": 1, "y": 2}, msg)

    @mock.patch("drama.process.Process._consumer")
    def test_should_get_messages_from_upstream(self, consumer):
        def poll(**kwargs):
            _TopicPartition = collections.namedtuple("TopicPartition", [])
            _ConsumerRecord = collections.namedtuple("ConsumerRecord", ["key", "value"])

            point_record = _ConsumerRecord(
                key=b"test-task-0",
                value=b'\nBLOCK"test-task-0.Point\x04\x02\x04\x08AVRO\xd8\x03{"namespace": "drama.examples.publisher.DemoSinglePublisherPoint", "name": "Point", "type": "record", "fields": [{"name": "x", "type": "int"}, {"name": "y", "type": "int"}]}',
            )

            stop_record = _ConsumerRecord(
                key=b"test-task-0",
                value=b"\x0cSIGNAL\x12undefined\x18POISSON_PILL\x12undefined\x12undefined",
            )

            return {_TopicPartition: [point_record, stop_record]}

        mocked_consumer = MagicMock()
        mocked_consumer.poll = poll

        consumer.return_value = mocked_consumer  # mock self._consumer() to avoid kafka produce

        records = self.process.get_from_upstream()
        self.assertTrue(len(records.keys()) == 1)
        self.assertEqual([{"x": 1, "y": 2}], records["point"])

    @mock.patch("drama.process.Process._consumer")
    def test_should_raise_exception_if_some_inputs_are_missing(self, consumer):
        def poll(**kwargs):
            _TopicPartition = collections.namedtuple("TopicPartition", [])
            _ConsumerRecord = collections.namedtuple("ConsumerRecord", ["key", "value"])

            stop_record = _ConsumerRecord(
                key=b"test-task-0",
                value=b"\x0cSIGNAL\x12undefined\x18POISSON_PILL\x12undefined\x12undefined",
            )

            return {_TopicPartition: [stop_record]}

        mocked_consumer = MagicMock()
        mocked_consumer.poll = poll

        consumer.return_value = mocked_consumer  # mock self._consumer() to avoid kafka producer

        with self.assertRaises(Exception):
            self.process.get_from_upstream()

    def tearDown(self) -> None:
        self.process.storage.remove_local_dir()


class ProcessWithTwoInputsOneUpstreamTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.process = Process(
            name="test-task-1",
            module="",
            params={},
            parent="test-workflow-2",
            inputs={"point_a": "test-task-0.PointA", "point_b": "test-task-0.PointB"},
        )

    @mock.patch("drama.process.Process._consumer")
    def test_should_get_all_messages_from_upstream(self, consumer):
        def poll(**kwargs):
            _TopicPartition = collections.namedtuple("TopicPartition", [])
            _ConsumerRecord = collections.namedtuple("ConsumerRecord", ["key", "value"])

            point_record_a = _ConsumerRecord(
                key=b"test-task-0",
                value=b'\nBLOCK$test-task-0.PointA\x04\x02\x04\x08AVRO\xda\x03{"namespace": "drama.examples.publisher.DemoSinglePublisherPoint", "name": "PointA", "type": "record", "fields": [{"name": "x", "type": "int"}, {"name": "y", "type": "int"}]}',
            )

            point_record_b = _ConsumerRecord(
                key=b"test-task-0",
                value=b'\nBLOCK$test-task-0.PointB\x04\x06\x08\x08AVRO\xda\x03{"namespace": "drama.examples.publisher.DemoSinglePublisherPoint", "name": "PointB", "type": "record", "fields": [{"name": "x", "type": "int"}, {"name": "y", "type": "int"}]}',
            )

            stop_record = _ConsumerRecord(
                key=b"test-task-0",
                value=b"\x0cSIGNAL\x12undefined\x18POISSON_PILL\x12undefined\x12undefined",
            )

            return {_TopicPartition: [point_record_a, point_record_b, stop_record]}

        mocked_consumer = MagicMock()
        mocked_consumer.poll = poll

        consumer.return_value = mocked_consumer  # mock self._consumer() to avoid kafka producer

        records = self.process.get_from_upstream()
        self.assertTrue(len(records.keys()) == 2)
        self.assertEqual([{"x": 1, "y": 2}], records["point_a"])
        self.assertEqual([{"x": 3, "y": 4}], records["point_b"])

    def tearDown(self) -> None:
        self.process.storage.remove_local_dir()


if __name__ == "__main__":
    unittest.main()
