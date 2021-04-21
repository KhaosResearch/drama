from drama.process import Process


def execute(pcs: Process):
    """
    Reads stream of 3D-Points with cartesian coordinates.
    """
    for key, p in pcs.poll_from_upstream():
        if key == "Points":
            pcs.info([f"Got point ({p['x']},{p['y']},{p['z']})"])
