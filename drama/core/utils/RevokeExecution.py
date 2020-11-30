from drama.models.messages import SignalMessage, SignalType
from drama.process import Process
from drama.servo import serialize


def execute(pcs: Process, **kwargs):
    """
    Sends a global interruption signal.
    """
    signal = SignalMessage(data=SignalType.INTE)
    serialized_message = serialize(signal.dict(), pcs.MESSAGE_SCHEMA)

    producer = pcs._producer()
    producer.send(topic=pcs.parent, value=serialized_message, key=bytes(pcs.parent, "utf-8"), **kwargs)
    producer.close()
