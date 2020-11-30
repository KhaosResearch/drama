from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, validator

from drama.models.task import Task, TaskRequest


class WorkflowRequest(BaseModel):
    tasks: List[TaskRequest]
    labels: List[str] = []
    metadata: dict = {}

    @validator("tasks")
    def task_names_not_duplicated(cls, v):
        names = [v.name for v in v]
        assert len(set(names)) == len(names), "Found duplicated tasks names in workflow"
        return v


class Workflow(BaseModel):
    id: str
    tasks: List[Task] = []
    labels: List[str] = []
    metadata: dict = {}
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    is_revoked: bool = False
