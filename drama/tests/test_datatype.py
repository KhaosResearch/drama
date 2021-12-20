import unittest
from dataclasses import dataclass

from drama.datatype import (
    AtomicType,
    DataType,
    get_dict,
    get_schema,
    is_boolean,
    is_datatype,
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

            class Config:
                namespace = "test_datatype"

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

        is_dict = {"my_number": 0, "my_string": "hello"}
        as_dict = get_dict(DataChuck(my_number=0))

        self.assertEqual(is_dict, as_dict)

    def test_should_generate_schema_with_default_values(self) -> None:
        @dataclass
        class DataChuck(DataType):
            my_number: int = is_integer()
            my_string: str = is_string(default="hello")

            class Config:
                namespace = "test_datatype"

        schema = {
            "fields": [{"name": "my_number", "type": "int"}, {"name": "my_string", "type": "string"}],
            "name": "DataChuck",
            "namespace": "test_datatype",
            "type": "record",
        }
        parsed_schema = get_schema(DataChuck(my_number=0))

        self.assertEqual(schema, parsed_schema)

    def test_should_generate_dict_with_all_default_values(self) -> None:
        @dataclass
        class DataChuck(DataType):
            my_number: int = is_integer(default=0)
            my_string: str = is_string(default="hello")

            class Config:
                namespace = "test_datatype"

        schema = {
            "fields": [{"name": "my_number", "type": "int"}, {"name": "my_string", "type": "string"}],
            "name": "DataChuck",
            "namespace": "test_datatype",
            "type": "record",
        }
        parsed_schema = get_schema(DataChuck())

        self.assertEqual(schema, parsed_schema)

    def test_should_generate_dict_with_list(self) -> None:
        @dataclass
        class DataChuck(DataType):
            my_list: list = is_list(items=AtomicType.String)

        is_dict = {"my_list": ["hello", "world"]}
        as_dict = get_dict(DataChuck(["hello", "world"]))

        self.assertEqual(is_dict, as_dict)

    def test_should_generate_schema_with_list(self) -> None:
        @dataclass
        class DataChuck(DataType):
            my_list: list = is_list(items=AtomicType.String)

            class Config:
                namespace = "test_datatype"

        schema = {
            "fields": [{"name": "my_list", "type": {"items": "string", "type": "array"}}],
            "name": "DataChuck",
            "namespace": "test_datatype",
            "type": "record",
        }
        parsed_schema = get_schema(DataChuck(["hello", "world"]))

        self.assertEqual(schema, parsed_schema)

    def test_should_generate_schema_with_complex_list(self) -> None:
        @dataclass
        class InnerDataChunk(DataType):
            my_number: int = is_integer()

            class Config:
                name = "InnerDataChunk"
                namespace = "test_datatype"

        @dataclass
        class DataChuck(DataType):
            my_list: list = is_list(items=InnerDataChunk)

            class Config:
                namespace = "test_datatype"

        schema = {
            "fields": [
                {
                    "name": "my_list",
                    "type": {
                        "type": "array",
                        "items": {
                            "fields": [{"name": "my_number", "type": "int"}],
                            "name": "InnerDataChunk",
                            "namespace": "test_datatype",
                            "type": "record",
                        },
                    },
                }
            ],
            "name": "DataChuck",
            "namespace": "test_datatype",
            "type": "record",
        }
        parsed_schema = get_schema(DataChuck([InnerDataChunk(0), InnerDataChunk(1)]))

        self.assertEqual(schema, parsed_schema)

    def test_should_generate_schema_with_inner_datatype(self) -> None:
        @dataclass
        class InnerDataChunk(DataType):
            my_number: int = is_integer()

            class Config:
                name = "InnerDataChunk"
                namespace = "test_datatype"

        @dataclass
        class DataChuck(DataType):
            my_data: DataType = is_datatype(InnerDataChunk)

            class Config:
                namespace = "test_datatype"

        inner_data_chunk_schema = {
            "fields": [{"name": "my_number", "type": "int"}],
            "name": "InnerDataChunk",
            "namespace": "test_datatype",
            "type": "record",
        }

        data_chunk_schema = {
            "fields": [{"name": "my_data", "type": inner_data_chunk_schema}],
            "name": "DataChuck",
            "namespace": "test_datatype",
            "type": "record",
        }
        parsed_schema = get_schema(DataChuck(InnerDataChunk(10)))

        self.assertEqual(data_chunk_schema, parsed_schema)

    def test_should_generate_schema_with_complex_inner_datatype(self) -> None:
        @dataclass
        class InnerInnerDataChunk(DataType):
            my_number: int = is_integer()

            class Config:
                name = "InnerInnerDataChunk"
                namespace = "test_datatype"

        @dataclass
        class InnerDataChunk(DataType):
            my_list: list = is_list(InnerInnerDataChunk)

            class Config:
                name = "InnerDataChunk"
                namespace = "test_datatype"

        @dataclass
        class DataChuck(DataType):
            my_data: DataType = is_datatype(InnerDataChunk)

            class Config:
                namespace = "test_datatype"

        inner_data_chunk_schema = {
            "fields": [
                {
                    "name": "my_list",
                    "type": {
                        "items": {
                            "fields": [{"name": "my_number", "type": "int"}],
                            "name": "InnerInnerDataChunk",
                            "namespace": "test_datatype",
                            "type": "record",
                        },
                        "type": "array",
                    },
                }
            ],
            "name": "InnerDataChunk",
            "namespace": "test_datatype",
            "type": "record",
        }

        data_chunk_schema = {
            "fields": [{"name": "my_data", "type": inner_data_chunk_schema}],
            "name": "DataChuck",
            "namespace": "test_datatype",
            "type": "record",
        }
        parsed_schema = get_schema(DataChuck(InnerDataChunk([1, 2, 3])))

        self.assertEqual(data_chunk_schema, parsed_schema)

    def test_should_raised_exception_when_non_default_arg_follows_default(self) -> None:
        with self.assertRaises(TypeError):

            @dataclass
            class DataChuck(DataType):
                my_number: int = is_integer(default=0)
                my_string: str = is_string()


if __name__ == "__main__":
    unittest.main()
