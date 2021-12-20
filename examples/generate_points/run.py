import time

from drama.models.task import Task
from drama.models.workflow import Workflow
from drama.worker.scheduler import Scheduler

task_one = Task(
    name="ComponentPointPublisher",
    module="examples/generate_points/catalog/PointPublisher.py",
    params={"x": 5, "y": 17},
)

task_two = Task(
    name="ComponentStreamingPointReader",
    module="examples/generate_points/catalog/StreamingPointReader.py",
    inputs={
        "Points": "ComponentPointPublisher.Point",
    },
)

task_three = Task(
    name="ComponentPointReader",
    module="examples/generate_points/catalog/PointReader.py",
    inputs={
        "Points": "ComponentPointPublisher.Point",
    },
)

workflow_request = Workflow(tasks=[task_one, task_two, task_three])

with Scheduler() as scheduler:
    workflow = scheduler.run(workflow_request)
    print(workflow)
    print(scheduler.status(workflow_id=workflow.id))
    time.sleep(5)
    print(scheduler.status(workflow_id=workflow.id))
