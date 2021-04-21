import csv
from typing import Dict

from drama.core.annotation import TaskMeta, annotation
from drama.core.model import SimpleTabularDataset
from drama.process import Process


@annotation(TaskMeta(name="ReadTSV", desc="Reads a TSV file", inputs=("TabularDataset", SimpleTabularDataset)))
def execute(pcs: Process, **kwargs):
    """
    Reads a TSV file.
    """
    inputs = pcs.get_from_upstream()

    # read inputs
    input_file: Dict[SimpleTabularDataset] = inputs["TabularDataset"][0]
    input_file_delimiter = input_file["delimiter"]
    input_file_resource = input_file["resource"]

    filepath = pcs.storage.get_file(input_file_resource)

    with open(filepath, "r") as reader:
        for row in csv.reader(reader, delimiter=input_file_delimiter):
            pcs.info(row)
