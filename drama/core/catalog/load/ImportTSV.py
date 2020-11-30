import csv
import os
import urllib.request
from pathlib import Path
from urllib.parse import urlparse

from drama.core.annotation import component
from drama.core.model import SimpleTabularDataset
from drama.process import Process


@component(outputs=SimpleTabularDataset)
def execute(pcs: Process, url: str, delimiter: str = "\t", comment: str = "#"):
    """
    Imports a tab-separated values file from an online resource given its url.

    Args:
        pcs (Process)

    Parameters:
        url (str): Public accessible resource
        delimiter (str): Line column delimiter. Defaults to "\t".
        comment (str). Character representing starting comment. Defaults to "#".
    """
    filename = Path(urlparse(url).path).name
    filepath = Path(pcs.storage.local_dir, filename)

    urllib.request.urlretrieve(url, filepath)

    out_tsv = Path(pcs.storage.local_dir, "out.tsv")

    def validate(csvfile):
        """
        De-comment input file.
        """
        for row in csvfile:
            raw = row.split(comment)[0].strip()
            if raw:
                yield raw

    with open(filepath, "r") as infile, open(out_tsv, "w", newline="") as outfile:
        reader = csv.reader(validate(infile), delimiter=delimiter)
        writer = csv.writer(outfile, delimiter=delimiter, lineterminator=os.linesep)

        for row in reader:
            writer.writerow(row)

    # send to remote storage
    dfs_dir = pcs.storage.put_file(out_tsv)

    # send to downstream
    output_simple_tabular_dataset = SimpleTabularDataset(resource=dfs_dir, delimiter="\t", file_format=".tsv")
    pcs.to_downstream(output_simple_tabular_dataset)

    return {"output": output_simple_tabular_dataset, "resource": dfs_dir}
