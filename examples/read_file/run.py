from drama.models.task import Task
from drama.models.workflow import Workflow
from drama.worker.scheduler import Scheduler

task_one = Task(
    name="ComponentImportFileOne",
    module="drama.core.catalog.load.ImportTSV",
    params={"url": "https://raw.githubusercontent.com/solidsnack/tsv/master/cities10.tsv"},
)

task_two = Task(
    name="ComponentImportFileTwo",
    module="drama.core.catalog.load.ImportTSV",
    params={"url": "https://raw.githubusercontent.com/solidsnack/tsv/master/cities10.tsv"},
)

task_three = Task(
    name="ComponentReadFile",
    module="drama.core.catalog.read.ReadTSV",
    inputs={
        "TabularDataset": "ComponentImportFileOne.SimpleTabularDataset",
    },
)

workflow_request = Workflow(tasks=[task_one, task_two, task_three])

with Scheduler() as scheduler:
    workflow = scheduler.run(workflow_request)
    print(workflow)

    import time

    time.sleep(15)
    print(scheduler.status(workflow_id=workflow.id))
