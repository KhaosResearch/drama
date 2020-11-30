import urllib.request
from pathlib import Path
from urllib.parse import urlparse

from drama.core.annotation import component
from drama.core.model import TempFile
from drama.process import Process


@component(outputs=TempFile)
def execute(pcs: Process, url: str, parameters: str = None, **kwargs):
    """
    Imports a file from an online resource given its url.

    Args:
        pcs (Process)

    Parameters:
        url (str): Public accessible resource
        parameters (str): GET parameters to append to url
    """
    filename = Path(urlparse(url).path).name
    filepath = Path(pcs.storage.local_dir, filename)

    # append GET parameters to url
    if parameters:
        url = url + parameters

    urllib.request.urlretrieve(url, filepath)

    # send to remote storage
    dfs_dir = pcs.storage.put_file(filepath)

    # send to downstream
    output_temp_file = TempFile(resource=dfs_dir)
    pcs.to_downstream(output_temp_file)

    return {"output": output_temp_file, "resource": dfs_dir}
