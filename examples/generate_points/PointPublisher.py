from dataclasses import dataclass

from drama.core.annotation import component
from drama.datatype import DataType, is_integer
from drama.process import Process


@dataclass
class Point(DataType):
    x: int = is_integer()
    y: int = is_integer()
    z: int = is_integer(default=0)


@component(outputs=Point)
def execute(pcs: Process, x: int, y: int):
    """
    Parameters:
        pcs (Process): `drama` process
        x (int): x-cartesian coordinate
        y (int): y-cartesian coordinate
    """
    pcs.info([f"Generating point ({x},{y},0)"])
    point = Point(x, y)

    # send to downstream
    for _ in range(10):
        pcs.to_downstream(point)
