from drama.models.task import Task
from drama.models.workflow import Workflow
from drama.worker.scheduler import Scheduler

task_import_file = Task(
    name="LoadIrisDataset",
    module="drama.core.catalog.load.ImportFile",
    params={
        "url": "https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv"
    }
)

workflow_request = Workflow(tasks=[task_import_file], metadata=dict(author="fran"))

with Scheduler() as scheduler:
    scheduler.run(workflow_request)
