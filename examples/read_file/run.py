from drama.models.task import TaskRequest
from drama.models.workflow import WorkflowRequest
from drama.worker import execute

task_one = TaskRequest(
    name="ComponentImportFileOne",
    module="drama.core.catalog.load.ImportTSV",
    params={"url": "https://raw.githubusercontent.com/solidsnack/tsv/master/cities10.tsv"},
)

task_two = TaskRequest(
    name="ComponentImportFileTwo",
    module="drama.core.catalog.load.ImportTSV",
    params={"url": "https://raw.githubusercontent.com/solidsnack/tsv/master/cities10.tsv"},
)

task_three = TaskRequest(
    name="ComponentReadFile",
    module="drama.core.catalog.read.ReadTSV",
    inputs={
        "TabularDataset": "ComponentImportFileOne.SimpleTabularDataset",
    },
)

workflow_request = WorkflowRequest(tasks=[task_one, task_two])

# executes workflow
workflow = execute(workflow_request)
