import math
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from kafka import KafkaConsumer, KafkaProducer
from starlette.requests import Request

from drama.config import settings
from drama.database import get_db_connection
from drama.logger import get_logger
from drama.manager import TaskManager, WorkflowManager
from drama.models.workflow import *
from drama.worker.scheduler import Scheduler

logger = get_logger(__name__)

router = APIRouter()


@router.post(
    "/run",
    name="Execute workflow",
    tags=["workflow"],
    response_model=WorkflowInDb,
    response_model_exclude_unset=True,
    responses={400: {"description": "x-token header invalid"}},
)
async def run(workflow_request: Workflow) -> WorkflowInDb:
    """
    Executes a collection of tasks.
    """
    logger.info("Received workflow request")
    with Scheduler() as scheduler:
        w = scheduler.run(workflow_request)
    return w


@router.get(
    "/status",
    name="Get workflow execution status",
    tags=["workflow"],
    response_model=WorkflowInDb,
    responses={400: {"description": "x-token header invalid"}},
)
async def status(
    id: str,
    db=Depends(get_db_connection),
) -> WorkflowInDb:
    """
    Returns execution status from execution id.
    """
    workflow = WorkflowManager(db).find_one(id=id)

    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {id} not found")
    else:
        logger.info(f"Found workflow from execution id {id}")

    workflow.tasks = TaskManager(db).find(parent=id)

    return workflow


@router.post(
    "/revoke",
    name="Cancel workflow execution",
    tags=["workflow"],
    response_model=WorkflowInDb,
    response_model_exclude_unset=True,
    responses={400: {"description": "x-token header invalid"}},
)
async def cancel_or_revoke(
    id: str,
    db=Depends(get_db_connection),
) -> WorkflowInDb:
    """
    Revokes the execution of a workflow, i.e., cancel the execution of pending tasks.
    """
    workflow = WorkflowManager(db).find_one(id=id)

    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {id} not found")
    else:
        logger.info(f"Found workflow from execution id {id}")

    if not workflow.is_revoked:
        with Scheduler() as scheduler:
            workflow = scheduler.revoke(workflow_id=workflow.id)

    return workflow


@router.post(
    "/topic",
    name="Sends a message thought a Kafka topic.",
    tags=["workflow"],
    responses={400: {"description": "x-token header invalid"}},
)
async def component(
    id: str,
    component: str,
    message: str,
    db=Depends(get_db_connection),
) -> None:
    """
    Sends a message though a Kafka topic. Useful for components that reads from a Kafka topic to allow interactivity from the user.
    """
    producer = KafkaProducer(bootstrap_servers=[settings.KAFKA_CONN])
    producer.send(topic=f"{id}-{component}", value=bytes(message, "utf-8"))
    producer.close()
