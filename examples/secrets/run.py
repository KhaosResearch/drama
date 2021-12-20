from drama.models.task import Task
from drama.models.workflow import Workflow
from drama.worker.scheduler import Scheduler

task_one = Task(
    name="ComponentParameter",
    module="examples/secrets/catalog/LeakSecrets.py",
    secrets=[
        dict(
            token="SECRET_TOKEN",
            secret="+aoJgK/BpkVGDldn9UBQ06RxG8eD2mgP/U4dZKmTpk3pyiTFfZhs8cE0e9jS8koxL21O2ZAwYNHjOMOgK/akBQ==",
        )
    ],
)

workflow_request = Workflow(tasks=[task_one])

with Scheduler() as scheduler:
    workflow = scheduler.run(workflow_request)
    print(workflow)
    print(scheduler.status(workflow_id=workflow.id))
