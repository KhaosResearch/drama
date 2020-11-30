from dataclasses import dataclass
from typing import Callable, List, Optional, Union


def component(
    inputs: Optional[Union[List[tuple], tuple]] = None, outputs: Optional[Union[dataclass, list]] = None, **kwargs
):
    def decorator(func: Callable):
        # required
        setattr(func, "inputs", inputs)
        setattr(func, "outputs", outputs)
        # optional
        for k, v in kwargs.items():
            setattr(func, k, v)
        return func

    return decorator
