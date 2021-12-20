import uuid
from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, validator
from pydantic.fields import Field

from drama.models.task import Task, TaskInDb


class WorkflowStatus(str, Enum):
    STATUS_UNKNOWN: str = "UNKNOWN"
    STATUS_REVOKED: str = "REVOKED"
    STATUS_PENDING: str = "PENDING"
    STATUS_RUNNING: str = "RUNNING"
    STATUS_FAILED: str = "FAILED"
    STATUS_DONE: str = "DONE"


class WorkflowMetadata(BaseModel):
    author: str = "anonymous"

    class Config:
        extra = "allow"


class Workflow(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4().hex))
    tasks: List[Task] = []
    secrets: List[str] = []
    labels: List[str] = []
    metadata: WorkflowMetadata = WorkflowMetadata()

    @validator("tasks")
    def task_names_not_duplicated(cls, v):
        names = [v.name for v in v]
        assert len(set(names)) == len(names), "Found duplicated tasks names in workflow"
        return v


class WorkflowInDb(BaseModel):
    id: str
    tasks: List[TaskInDb] = []
    secrets: List[str] = []
    labels: List[str] = []
    metadata: dict = {}
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    status: WorkflowStatus = WorkflowStatus.STATUS_UNKNOWN
    is_revoked: bool = False

    class Config:
        use_enum_values = True
