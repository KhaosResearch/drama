import unittest
from dataclasses import dataclass

from drama.datatype import (
    DataType,
    get_dict,
    get_schema,
    is_boolean,
    is_float,
    is_integer,
    is_list,
    is_string,
)


class TabularDatasetTestCase(unittest.TestCase):
    def test_should_generate_schema(self) -> None:
        @dataclass
        class DataChuck(DataType):
            my_number: int = is_integer()
            my_float: float = is_float()
            my_string: str = is_string()
            my_boolean: bool = is_boolean()
            my_list: list = is_list()

        schema = {
            "namespace": "test_datatype",
            "name": "DataChuck",
            "type": "record",
            "fields": [
                {"name": "my_number", "type": "int"},
                {"name": "my_float", "type": "float"},
                {"name": "my_string", "type": "string"},
                {"name": "my_boolean", "type": "boolean"},
                {"name": "my_list", "type": {"type": "array", "items": "int"}},
            ],
        }
        parsed_schema = get_schema(
            DataChuck(
                my_number=0,
                my_float=1.1,
                my_string="char",
                my_boolean=True,
                my_list=[1, 2, 3],
            )
        )

        self.assertEqual(schema["name"], parsed_schema["name"])
        self.assertEqual(schema["type"], parsed_schema["type"])
        self.assertEqual(schema["fields"], parsed_schema["fields"])

    def test_should_generate_dict_with_default_values(self) -> None:
        @dataclass
        class DataChuck(DataType):
            my_number: int = is_integer()
            my_string: str = is_string(default="hello")

        schema = {"my_number": 0, "my_string": "hello"}
        as_dict = get_dict(DataChuck(my_number=0))

        self.assertEqual(schema, as_dict)

    def test_should_generate_dict_with_all_default_values(self) -> None:
        @dataclass
        class DataChuck(DataType):
            my_number: int = is_integer(default=0)
            my_string: str = is_string(default="hello")

        schema = {"my_number": 0, "my_string": "hello"}
        as_dict = get_dict(DataChuck())

        self.assertEqual(schema, as_dict)

    def test_should_raised_exception_when_non_default_arg_follows_default(self) -> None:
        with self.assertRaises(TypeError):

            @dataclass
            class DataChuck(DataType):
                my_number: int = is_integer(default=0)
                my_string: str = is_string()


if __name__ == "__main__":
    unittest.main()
