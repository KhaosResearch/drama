from drama.models.task import Task
from drama.models.workflow import Workflow
from drama.worker.scheduler import Scheduler

task_one = Task(
    name="ComponentParameter",
    module="drama.core.utils.DynamicParameter",
)

workflow_request = Workflow(tasks=[task_one])

with Scheduler() as scheduler:
    workflow = scheduler.run(workflow_request)
    print(workflow)
    print(scheduler.status(workflow_id=workflow.id))
