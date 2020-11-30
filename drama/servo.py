import io

from fastavro import schemaless_reader, schemaless_writer


def serialize(dict_data: dict, servo_schema: dict) -> bytes:
    """
    Serialize `dict_data` using `servo_schema` with Apache Avro.
    """
    with io.BytesIO() as bytes_writer:
        schemaless_writer(bytes_writer, servo_schema, dict_data)
        serialized_data = bytes_writer.getvalue()
    return serialized_data


def deserialize(serialized_data: bytes, servo_schema: dict) -> dict:
    """
    Deserialize `serialized_data` using `servo_schema` with Apache Avro.
    """
    with io.BytesIO() as bytes_writer:
        bytes_writer.write(serialized_data)
        bytes_writer.seek(0)
        message = schemaless_reader(bytes_writer, servo_schema)
    return message
