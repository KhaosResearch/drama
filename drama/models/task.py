import base64
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from nacl.encoding import Base64Encoder
from nacl.public import PrivateKey, SealedBox
from pydantic import BaseModel, validator

from drama.config import settings
from drama.storage.base import Resource


class TaskResult(BaseModel):
    message: Optional[Any] = None
    files: Optional[Union[List[Resource], List[Dict[str, Resource]]]] = []
    log: Optional[Resource] = None


class TaskStatus(str, Enum):
    STATUS_UNKNOWN: str = "UNKNOWN"
    STATUS_PENDING: str = "PENDING"
    STATUS_RUNNING: str = "RUNNING"
    STATUS_FAILED: str = "FAILED"
    STATUS_DONE: str = "DONE"


class TaskUnsealedSecret(BaseModel):
    token: str
    secret: str


def base64_to_bytes(key: str) -> bytes:
    return base64.b64decode(key.encode("utf-8"))


class TaskSecret(BaseModel):
    token: str
    secret: str

    def unseal(self, sk: str) -> TaskUnsealedSecret:
        private = PrivateKey(base64_to_bytes(sk))
        unseal_box = SealedBox(private)
        plaintext = unseal_box.decrypt(base64_to_bytes(self.secret))
        return TaskUnsealedSecret(token=self.token, secret=plaintext.decode("utf-8"))


class TaskOpts(BaseModel):
    on_fail_force_interruption: bool = True
    on_fail_remove_local_dir: bool = True
    queue_name: Optional[str] = None


class Task(BaseModel):
    name: str
    module: str
    params: dict = {}
    inputs: dict = {}
    labels: List[str] = []
    secrets: List[TaskSecret] = []
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


class TaskInDb(Task):
    id: str
    name: str = ""
    parent: str = ""
    module: str = ""
    params: dict = {}
    inputs: dict = {}
    labels: List[str] = []
    secrets: List[TaskSecret] = []
    options: TaskOpts = TaskOpts()
    metadata: dict = {}
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    result: Optional[TaskResult] = None
    status: TaskStatus = TaskStatus.STATUS_UNKNOWN

    class Config:
        use_enum_values = True
