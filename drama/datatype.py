from dataclasses import MISSING, Field, asdict, dataclass, field
from enum import Enum


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
    def key(self):
        return type(self).__name__


def is_list(items: AtomicType = AtomicType.Integer, default: list = MISSING):  # type: ignore
    return field(
        default_factory=default,
        metadata={"type": AtomicType.List, "items": items},
    )


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
    Convert a field to dictionary. todo Support Nested fields
    :returns: Field class as dictionary following the Avro specification.
    """
    if field.type == list:
        _items = field.metadata["items"].value
        v = {"name": field.name, "type": {"type": "array", "items": _items}}
    else:
        _type = field.metadata["type"].value
        v = {"name": field.name, "type": _type}
    return v


def get_dict(data: DataType) -> dict:
    """
    Returns a dictionary associated with the data type.
    :returns: Dictionary of the class.
    """
    return asdict(data)


def get_schema(data: DataType) -> dict:
    """
    Returns the Avro schema associated with the data type.
    :returns: Avro Schema of the class.
    """
    fields: dict = _fields(data)
    schema: list = []

    for field in fields.values():
        schema.append(_as_dict(field))

    return dict(
        namespace=data.__module__,
        name=data.__class__.__name__,
        type="record",
        fields=schema,
    )
