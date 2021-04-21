import time

from drama.manager import TaskManager, WorkflowManager
from drama.models.task import TaskRequest
from drama.models.workflow import WorkflowRequest
from drama.worker import execute, revoke

task_one = TaskRequest(
    name="ComponentPointReader1",
    module="examples/generate_points/catalog/PointReader.py",
    inputs={
        "Points": "ComponentPointPublisher.Point",
    },
)

task_two = TaskRequest(
    name="ComponentPointReader2",
    module="examples/generate_points/catalog/StreamingPointReader.py",
    inputs={
        "Points": "ComponentPointPublisher.Point",
    },
)

workflow_request = WorkflowRequest(tasks=[task_one, task_two])

# executes workflow
workflow = execute(workflow_request)

time.sleep(5)

print(TaskManager().find({"parent": workflow.id}))

time.sleep(10)

revoke(workflow.id)

# gets results
for _ in range(100):
    time.sleep(5)
    print(TaskManager().find({"parent": workflow.id}))
    print(WorkflowManager().find_one({"id": workflow.id}))
