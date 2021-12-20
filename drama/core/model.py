from dataclasses import dataclass

from drama.datatype import DataType, is_string
from drama.storage.base import Resource


@dataclass
class TempFile(DataType):
    resource: Resource = is_string()


@dataclass
class CompressedFile(DataType):
    resource: Resource = is_string()
    file_format: str = is_string(".zip")


@dataclass
class _BaseSimpleTabularDataset(DataType):
    resource: Resource = is_string()
    delimiter: str = is_string()


@dataclass
class SimpleTabularDataset(_BaseSimpleTabularDataset):
    encoding: str = is_string("utf-8")
    file_format: str = is_string(".csv")


@dataclass
class DynamicParameter(DataType):
    value: str = is_string()
