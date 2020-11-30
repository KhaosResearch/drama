from dataclasses import dataclass

from drama.datatype import DataType, is_string


@dataclass
class TempFile(DataType):
    resource: str = is_string()


@dataclass
class CompressedFile(DataType):
    resource: str = is_string()
    file_format: str = is_string(".zip")


@dataclass
class _BaseSimpleTabularDataset(DataType):
    resource: str = is_string()
    delimiter: str = is_string()


@dataclass
class SimpleTabularDataset(_BaseSimpleTabularDataset):
    encoding: str = is_string("utf-8")
    file_format: str = is_string(".csv")
