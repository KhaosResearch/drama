from drama.models.task import TaskRequest
from drama.models.workflow import WorkflowRequest
from drama.worker import execute

task_one = TaskRequest(
    name="ComponentPointPublisher",
    module="examples/generate_points/catalog/PointPublisher.py",
    params={"x": 5, "y": 17},
)

task_two = TaskRequest(
    name="ComponentStreamingPointReader",
    module="examples/generate_points/catalog/StreamingPointReader.py",
    inputs={
        "Points": "ComponentPointPublisher.Point",
    },
)

task_three = TaskRequest(
    name="ComponentPointReader",
    module="examples/generate_points/catalog/PointReader.py",
    inputs={
        "Points": "ComponentPointPublisher.Point",
    },
)

workflow_request = WorkflowRequest(tasks=[task_one, task_two, task_three])

# executes workflow
workflow = execute(workflow_request)
