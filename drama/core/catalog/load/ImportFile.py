import urllib.request
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from drama.core.annotation import TaskMeta, annotation
from drama.core.model import TempFile
from drama.models.task import TaskResult
from drama.process import Process
from drama.storage.base import NotValidScheme


@annotation(
    TaskMeta(
        name="ImportFile",
        desc="Imports a file from an online resource given its url",
        outputs=TempFile,
        params=[("url", str), ("parameters", str)],
    )
)
def execute(pcs: Process, url: str, parameters: Optional[str] = None, **kwargs) -> TaskResult:
    """
    Imports a file from an online resource given its url.

    Args:
        pcs (Process)

    Parameters:
        url (str): Public accessible resource
        parameters (str): GET parameters to append to url
    """
    filename = Path(urlparse(url).path).name

    try:
        filepath = pcs.storage.get_file(url)
    except (NotValidScheme, FileNotFoundError):
        pcs.warn("No valid scheme was provided")
        filepath = Path(pcs.storage.local_dir, filename)
        if parameters:
            url = url + parameters
        urllib.request.urlretrieve(url, filepath)

    # send to remote storage
    dfs_dir = pcs.storage.put_file(filepath)

    # send to downstream
    output_temp_file = TempFile(resource=dfs_dir)
    pcs.to_downstream(output_temp_file)

    return TaskResult(files=[dfs_dir])
