from fastapi import APIRouter, Depends, HTTPException

from drama.database import get_db_connection
from drama.logger import get_logger
from drama.manager import TaskManager, WorkflowManager
from drama.models.workflow import *
from drama.worker import execute
from drama.worker.executor import revoke

logger = get_logger(__name__)

router = APIRouter()


@router.post(
    "/run",
    name="Execute workflow",
    tags=["workflow"],
    response_model=Workflow,
    response_model_exclude_unset=True,
    responses={400: {"description": "x-token header invalid"}},
)
async def run(workflow_request: WorkflowRequest) -> Workflow:
    """
    Executes a collection of tasks.
    """
    logger.info("Received workflow request")
    workflow = execute(workflow_request)
    return workflow


@router.get(
    "/status",
    name="Get workflow execution status",
    tags=["workflow"],
    response_model=Workflow,
    responses={400: {"description": "x-token header invalid"}},
)
async def get(
    id: str,
    db=Depends(get_db_connection),
) -> Workflow:
    """
    Returns execution status from execution id.
    """
    workflow = WorkflowManager(db).find_one({"id": id})

    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {id} not found")
    else:
        logger.info(f"Found workflow from execution id {id}")

    workflow.tasks = TaskManager(db).find({"parent": id})

    return workflow


@router.post(
    "/revoke",
    name="Cancel workflow execution",
    tags=["workflow"],
    response_model=Workflow,
    response_model_exclude_unset=True,
    responses={400: {"description": "x-token header invalid"}},
)
async def cancel_or_revoke(
    id: str,
    db=Depends(get_db_connection),
) -> Workflow:
    """
    Revokes the execution of a workflow, i.e., cancel the execution of pending tasks.
    """
    workflow = WorkflowManager(db).find_one({"id": id})

    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {id} not found")
    else:
        logger.info(f"Found workflow from execution id {id}")

    if not workflow.is_revoked:
        workflow = revoke(id)

    return workflow
