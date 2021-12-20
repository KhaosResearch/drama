from dataclasses import MISSING, Field, asdict, dataclass, field
from enum import Enum
from typing import List, Optional, Type, Union


class AtomicType(Enum):
    String = "string"
    Integer = "int"
    Float = "float"
    List = "array"
    Boolean = "boolean"


@dataclass
class DataType:
    """
    Dataclass used for serialization/deserialization with AVRO.
    """

    @property
    def name(self):
        return type(self).__name__

    # We can override some configurations next
    # Useful when working with complex dataclasses
    class Config:
        name: Optional[str] = None
        namespace: Optional[str] = None
        schema: Optional[dict] = None


def is_list(items: Union[AtomicType, Type[DataType]] = AtomicType.Integer, default: list = MISSING):  # type: ignore
    return field(
        default_factory=default,
        metadata={"type": AtomicType.List, "items": items},
    )


def is_datatype(item: Type[DataType], default: Type[DataType] = MISSING):  # type: ignore
    return field(default=default, metadata={"type": DataType, "item": item})


def is_integer(default: int = MISSING):  # type: ignore
    return field(default=default, metadata={"type": AtomicType.Integer})


def is_float(default: float = MISSING):  # type: ignore
    return field(default=default, metadata={"type": AtomicType.Float})


def is_string(default: str = MISSING):  # type: ignore
    return field(default=default, metadata={"type": AtomicType.String})


def is_boolean(default: str = MISSING):  # type: ignore
    return field(default=default, metadata={"type": AtomicType.Boolean})


def _fields(data: DataType) -> dict:
    """
    Return the fields of the data type.
    :returns: Dictionary with attributes of the class.
    """
    return getattr(data, "__dataclass_fields__", {})


def _as_dict(field: Field) -> dict:
    """
    Convert a field to dictionary.
    :returns: Field class as dictionary following the Avro specification.
    """
    if field.type == list:
        item = field.metadata["items"]
        # Ideally, we should check if `item` is instance of `DataType`, i.e., isinstance(item, DataType)
        # But this check fails for some reason, so let's ask for forgiveness, not permission
        try:
            value = item.value  # if `item` is not type `Field` then `AttributeError` exception will raise
            v = {"name": field.name, "type": {"type": "array", "items": value}}
        except AttributeError:
            v = {"name": field.name, "type": {"type": "array", "items": get_schema(item)}}
    elif field.type == DataType:
        item = field.metadata["item"]
        v = {"name": field.name, "type": get_schema(item)}
    else:
        value = field.metadata["type"].value
        v = {"name": field.name, "type": value}
    return v


def get_dict(data: DataType) -> dict:
    """
    Returns a dictionary associated with the data type.
    :returns: Dictionary of the class.
    """
    return asdict(data)


def _get_fields_schema(fields: dict) -> list:
    """
    Returns Avro schema of a field.
    """
    _fields: List[dict] = []

    for _field in fields.values():
        _field_as_dict = _as_dict(_field)
        _fields.append(_field_as_dict)

    return _fields


def get_schema(data: DataType) -> dict:
    """
    Returns the Avro schema associated with the DataType.
    :returns: Avro Schema of the class.
    """
    schema = getattr(data.Config, "schema", None)
    if schema:
        return schema

    fields: dict = _fields(data)
    fields_schema = _get_fields_schema(fields)

    schema = dict(
        namespace=getattr(data.Config, "namespace", None) or data.__module__,
        name=getattr(data.Config, "name", None) or data.__class__.__name__,
        type="record",
        fields=fields_schema,
    )

    return schema
