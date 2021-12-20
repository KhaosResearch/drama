from drama.process import Process


def execute(pcs: Process):
    """
    Leaks secrets. Do not use in production!
    """
    for secret in pcs.secrets:
        pcs.info([f"{secret.token}={secret.secret}"])
