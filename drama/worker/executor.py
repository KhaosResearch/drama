import uuid
from datetime import datetime

import dramatiq

from drama.config import settings
from drama.database import get_db_connection
from drama.logger import get_logger
from drama.manager import TaskManager, WorkflowManager
from drama.models.task import Task, TaskRequest, TaskStatus
from drama.models.workflow import Workflow, WorkflowRequest
from drama.worker import process_failure, process_succeeded, process_task

logger = get_logger(__name__)


def execute(workflow_request: WorkflowRequest) -> Workflow:
    """
    Send workflow request to main `drama` actor.
    Workflow is divided into individual tasks and processed by the former actor.
    """
    workflow_id = str(uuid.uuid4().hex)
    logger.info(f"Generating workflow id {workflow_id}")

    db = get_db_connection()

    for task_request in workflow_request.tasks:
        execute_task(task_request, workflow_id=workflow_id)

    # creates workflow on database
    workflow = WorkflowManager(db).create_or_update_from_id(
        workflow_id,
        labels=workflow_request.labels,
        metadata=workflow_request.metadata,
        created_at=datetime.now(),
    )

    return workflow


def revoke(workflow_id: str) -> Workflow:
    """
    Cancel workflow execution.
    """
    logger.debug(f"Revoking workflow id {workflow_id}")

    db = get_db_connection()

    # executes a new task to revoke workflow
    task_revoke = TaskRequest(
        name="RevokeExecution",
        module="drama.core.utils.RevokeExecution",
    )
    execute_task(task_revoke, workflow_id=workflow_id)

    # updates workflow on database
    workflow = WorkflowManager(db).create_or_update_from_id(workflow_id, updated_at=datetime.now(), is_revoked=True)

    return workflow


def execute_task(task_request: TaskRequest, workflow_id: str) -> Task:
    """
    Send task request to main `drama` actor.
    """
    db = get_db_connection()

    # appends workflow id to task
    task_dict = task_request.dict()
    task_dict["parent"] = workflow_id

    # triggers actor execution
    _message = process_task.message_with_options(
        args=(task_dict,),
        on_failure=process_failure,
        on_success=process_succeeded,
    )
    _message = _message.copy(
        queue_name=settings.DEFAULT_ACTOR_OPTS.queue_name,
    )

    broker = dramatiq.get_broker()
    message = broker.enqueue(_message)

    # creates task on database
    task = TaskManager(db).create_or_update_from_id(
        message.message_id,
        name=task_request.name,
        module=task_request.module,
        parent=workflow_id,
        params=task_request.params,
        inputs=task_request.inputs,
        labels=task_request.labels,
        metadata=task_request.metadata,
        status=TaskStatus.STATUS_PENDING,
        created_at=datetime.now(),
    )

    return task
