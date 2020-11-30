import os
import unittest

from drama.servo import deserialize, serialize

ABS_DIRNAME = os.path.dirname(os.path.abspath(__file__))


class ServoTestCase(unittest.TestCase):
    weather_schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "tests",
        "type": "record",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "time", "type": "long"},
            {"name": "temp", "type": "int"},
        ],
    }

    def test_can_serialize_dict(self):
        record = {
            u"station": u"012650-99999",
            u"temp": 111,
            u"time": 1433275478,
        }
        b_record = serialize(record, self.weather_schema)

        self.assertEqual(b_record, b"\x18012650-99999\xac\xb1\xf0\xd6\n\xde\x01")

    def test_can_deserialize_bytes(self):
        b_record = b"\x18012650-99999\xac\xb1\xf0\xd6\n\xde\x01"
        record = deserialize(b_record, self.weather_schema)

        self.assertEqual(
            record,
            {"station": "012650-99999", "temp": 111, "time": 1433275478},
        )


if __name__ == "__main__":
    unittest.main()
