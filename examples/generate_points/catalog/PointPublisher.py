import random
from dataclasses import dataclass

from drama.datatype import DataType, is_integer
from drama.process import Process


@dataclass
class Point(DataType):
    x: int = is_integer()
    y: int = is_integer()
    z: int = is_integer(default=0)


def execute(pcs: Process, x: int, y: int):
    """
    Parameters:
        x (int): x-cartesian coordinate
        y (int): y-cartesian coordinate
    """
    pcs.info([f"Generating point ({x},{y},?)"])

    # send to downstream
    for i in range(10):
        z = random.randint(0, 10)
        point = Point(x, y, z)

        pcs.info([f"Sending {i}"])
        pcs.to_downstream(point)
