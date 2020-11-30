import csv
from typing import Dict

from drama.core.annotation import component
from drama.core.model import SimpleTabularDataset
from drama.process import Process


@component(inputs=("TabularDataset", SimpleTabularDataset))
def execute(pcs: Process, **kwargs):
    """
    Reads a TSV file.
    """
    inputs = pcs.get_from_upstream()

    # read inputs
    input_file: Dict[SimpleTabularDataset] = inputs["TabularDataset"][0]
    input_file_delimiter = input_file["delimiter"]
    input_file_resource = input_file["resource"]

    with open(input_file_resource, "r") as reader:
        reader = csv.reader(reader, delimiter=input_file_delimiter)
        for row in reader:
            pcs.info(row)
