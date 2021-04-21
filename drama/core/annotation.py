from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple, Type, Union


@dataclass
class TaskMeta:
    """
    Metadata associated to a component.
    """

    name: str
    desc: str
    inputs: Optional[Union[List[tuple], tuple]] = None
    outputs: Optional[Union[dataclass, list]] = None
    params: Optional[List[Tuple[str, Type]]] = None


def annotation(task_metadata: Union[TaskMeta, Type[TaskMeta]], **kwargs):
    def decorator(func: Callable):
        setattr(func, "__meta__", task_metadata)
        for k, v in kwargs.items():
            setattr(func, k, v)
        return func

    return decorator
