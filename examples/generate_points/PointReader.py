from drama.core.annotation import component
from drama.process import Process
from drama.tests.test_process import Point


@component(inputs=("Points", Point))
def execute(pcs: Process):
    """
    Reads 3D-Point with cartesian coordinates.
    """
    inputs = pcs.get_from_upstream()

    for p in inputs["Points"]:
        pcs.info([f"Got point ({p['x']},{p['y']},{p['z']})"])
