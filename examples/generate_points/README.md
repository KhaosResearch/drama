### Custom components development
 
In this example, we will build a workflow that generates a set of points defined in _(x,y,z)_ coordinates (with fixed _z_ value).

#### File structure

Let's create a new directory named `catalog` with the following structure:

```
.
└── catalog
    ├── PointPublisher.py
    └── PointReader.py
```

#### Random point generation

Let's see the file `catalog/PointPublisher.py`.

```python
from dataclasses import dataclass

from drama.datatype import DataType, is_integer
from drama.process import Process


@dataclass
class Point(DataType):
    x: int = is_integer()
    y: int = is_integer()
    z: int = is_integer(default=10)


def execute(pcs: Process, x: int, y: int) -> None:
    """
    :param x: (int) x-cartesian coordinate
    :param y: (int) y-cartesian coordinate
    """
    pcs.info([f"Generating point ({x},{y},0)"])
    point = Point(x, y)

    # send to downstream
    for _ in range(10):
        pcs.to_downstream(point)
```

Each file represents an isolated task with its own `execute()` function. It receives a Process class following an arbitrary number of parameters.

```diff
from dataclasses import dataclass

from drama.datatype import DataType, is_integer
+from drama.process import Process


@dataclass
class Point(DataType):
    x: int = is_integer()
    y: int = is_integer()
    z: int = is_integer(default=10)


+def execute(pcs: Process, x: int, y: int) -> None:
    """
    :param x: (int) x-cartesian coordinate
    :param y: (int) y-cartesian coordinate
    """
    pcs.info([f"Generating point ({x},{y},0)"])
    point = Point(x, y)

    # send to downstream
    for _ in range(10):
        pcs.to_downstream(point)
```

The Process class provides several utility methods. For example, `pcs.info` will log a message and `pcs.to_dowstream` sends data to the next component in the workflow.

Under the hood, the Process class will create a named temporal file in the system for debugging messages, set up the data file storage, etc.

```diff
from dataclasses import dataclass

from drama.datatype import DataType, is_integer
from drama.process import Process


@dataclass
class Point(DataType):
    x: int = is_integer()
    y: int = is_integer()
    z: int = is_integer(default=10)


def execute(pcs: Process, x: int, y: int) -> None:
    """
    :param x: (int) x-cartesian coordinate
    :param y: (int) y-cartesian coordinate
    """
+   pcs.info([f"Generating point ({x},{y},0)"])
    point = Point(x, y)

    # send to downstream
    for _ in range(10):
+       pcs.to_downstream(point)
```

We can define a dataclass, `Point`, to holds the _(x,y,z)_ coordinates.

This new class inherits from `drama.datatype.DataType` and is used for encoding purposes:

```diff
from dataclasses import dataclass

+from drama.datatype import DataType, is_integer
from drama.process import Process


+@dataclass
+class Point(DataType):
+   x: int = is_integer()
+   y: int = is_integer()
+   z: int = is_integer(default=10)


def execute(pcs: Process, x: int, y: int) -> None:
    """
    :param x: (int) x-cartesian coordinate
    :param y: (int) y-cartesian coordinate
    """
    pcs.info([f"Generating point ({x},{y},0)"])
+   point = Point(x, y)

    # send to downstream
    for _ in range(10):
+       pcs.to_downstream(point)
```


#### Reading points

Let's now take a look at the file `catalog/PointReader.py`.

```python
from drama.process import Process


def execute(pcs: Process):
    inputs = pcs.get_from_upstream()

    for p in inputs["Points"]:
        pcs.info([f"Got point ({p['x']},{p['y']},{p['z']})"])
```

It contains an `execute()` function receiving a Process class following required and optional parameters, if any:

```diff
+from drama.process import Process


+def execute(pcs: Process):
    inputs = pcs.get_from_upstream()

    for p in inputs["Points"]:
        pcs.info([f"Got point ({p['x']},{p['y']},{p['z']})"])
```

The `pcs.get_from_upstream` method from the Process class is used to get data from the input task(s).
It waits for all messages from input task(s) and returns a dictionary with results. 

```diff
from drama.process import Process


def execute(pcs: Process):
+   inputs = pcs.get_from_upstream()

    for p in inputs["Points"]:
        pcs.info([f"Got point ({p['x']},{p['y']},{p['z']})"])
```

#### Composing the workflow

Finally, let's see the file `runner.py`

```python
from drama.models.task import TaskRequest
from drama.models.workflow import WorkflowRequest
from drama.worker import execute

task_one = TaskRequest(
    name="ComponentPointPublisher",
    module="examples/generate_points/catalog/PointPublisher.py",
    params={"x": 5, "y": 17},
)

task_two = TaskRequest(
    name="ComponentPointReader",
    module="examples/generate_points/catalog/PointReader.py",
    inputs={"Points": "ComponentPointPublisher.Point",},
)

workflow = WorkflowRequest(tasks=[task_one, task_two])

execute(workflow)
```

A `TaskRequest` object defines the name provided to the task, its parameters and inputs, and the path to the module which will be imported.

> Other fields, such as labels and metadata, can also be defined in a task request.

```diff
+from drama.models.task import TaskRequest
+from drama.models.workflow import WorkflowRequest
from drama.worker import execute

+task_one = TaskRequest(
+   name="ComponentPointPublisher",
+   module="examples/generate_points/catalog/PointPublisher.py",
+   params={"x": 5, "y": 17},
+)

+task_two = TaskRequest(
+   name="ComponentPointReader",
+   module="examples/generate_points/catalog/PointReader.py",
+   inputs={"Points": "ComponentPointPublisher.Point",},
+)

workflow = WorkflowRequest(tasks=[task_one, task_two])

execute(workflow)
```

A `WorkflowRequest` encapsulates tasks. We will send it to the actor, which will prepare each task and send it to the workers in a serialized form. The actor will also take care of exceptions, update task(s) state(s), write results to the database, etc.

```diff
from drama.models.task import TaskRequest
from drama.models.workflow import WorkflowRequest
+from drama.worker import execute

task_one = TaskRequest(
    name="ComponentPointPublisher",
    module="examples/generate_points/catalog/PointPublisher.py",
    params={"x": 5, "y": 17},
)

task_two = TaskRequest(
    name="ComponentPointReader",
    module="examples/generate_points/catalog/PointReader.py",
    inputs={"Points": "ComponentPointPublisher.Point",},
)

+workflow = WorkflowRequest(tasks=[task_one, task_two])

+execute(workflow)
```

#### Testing

> Workflows using relative module imports require setting **<kbd>API_DEBUG=1</kbd> in the environment** for security reasons:
> ```console 
> $ API_DEBUG=1 poetry run drama worker --processes 4
> ```

From the root path of the project, use:

```console
$ poetry run python examples/generate_points/run.py
```
