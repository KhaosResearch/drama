from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel


class MessageType(str, Enum):
    BLOCK = "BLOCK"
    SIGNAL = "SIGNAL"


class SignalType(bytes, Enum):
    STOP = b"POISSON_PILL"
    INTE = b"INTERRUPTION"


class Servo(str, Enum):
    AVRO = "AVRO"


class Message(BaseModel):
    type: Union[MessageType, str] = MessageType.BLOCK
    key: Optional[str] = None
    data: Optional[bytes] = None
    schem: Optional[str] = None
    servo: Union[Servo, str] = Servo.AVRO

    class Config:
        use_enum_values = True


class SignalMessage(BaseModel):
    type: Union[MessageType, str] = MessageType.SIGNAL
    data: Optional[SignalType] = None

    class Config:
        use_enum_values = True
