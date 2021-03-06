from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, validator

from drama.storage.base import Resource


class TaskResult(BaseModel):
    message: Optional[Any] = None
    files: Optional[Union[List[Resource], List[Dict[str, Resource]]]] = []
    log: Optional[Resource] = None


class TaskStatus(str, Enum):
    STATUS_PENDING: str = "PENDING"
    STATUS_RUNNING: str = "RUNNING"
    STATUS_FAILED: str = "FAILED"
    STATUS_DONE: str = "DONE"


class TaskOpts(BaseModel):
    on_fail_force_interruption: bool = True
    on_fail_remove_local_dir: bool = True


class TaskRequest(BaseModel):
    name: str
    module: str
    params: dict = {}
    inputs: dict = {}
    labels: List[str] = []
    options: TaskOpts = TaskOpts()
    metadata: dict = {}

    @validator("name")
    def name_does_not_contain_spaces(cls, v):
        assert " " not in v, "name must not contain spaces"
        return v

    @validator("name")
    def name_does_not_contain_dots(cls, v):
        assert "." not in v, "name must not contain dots"
        return v

    @validator("inputs")
    def input_values_does_not_form_valid_identifier(cls, v):
        assert all(["." in v for v in v.values()]), "inputs values must form valid identifier (<task>.<output>)"
        return v


class Task(TaskRequest):
    id: str
    name: str = ""
    module: str = ""
    parent: str = ""
    params: dict = {}
    inputs: dict = {}
    labels: List[str] = []
    options: TaskOpts = TaskOpts()
    metadata: dict = {}
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    result: Optional[TaskResult] = None
    status: Optional[TaskStatus] = None

    class Config:
        use_enum_values = True
