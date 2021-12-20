import traceback
from datetime import datetime
from typing import Callable
import time 

import dramatiq
from dramatiq import MessageProxy
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.middleware import CurrentMessage

from drama.config import settings
from drama.database import get_db_connection
from drama.logger import get_logger
from drama.manager import TaskManager, WorkflowManager
from drama.models.task import TaskResult, TaskSecret, TaskStatus
from drama.models.workflow import WorkflowStatus
from drama.process import Process
from drama.storage.helpers import get_available_storage
from drama.worker.helpers import get_process_func

logger = get_logger(__name__)

logger.info("Setting up RabbitMQ broker")
broker = RabbitmqBroker(url=settings.RABBIT_DNS)
broker.add_middleware(CurrentMessage())

dramatiq.set_broker(broker)

logger.info(f"Worker connected to queue: {settings.DEFAULT_ACTOR_OPTS.queue_name}")


@dramatiq.actor(**settings.DEFAULT_ACTOR_OPTS.dict())
def worker(task_request: dict, workflow_id: str):
    """
    Main `drama` actor.
    Executes an arbitrary function defined by a task and updates its state.
    """
    message = CurrentMessage.get_current_message()

    logger.info(f"Processing task {message.message_id}")

    # Required task attributes.
    task_id = message.message_id
    task_name = task_request["name"]
    task_module = task_request["module"]
    task_author = task_request["metadata"]["author"]

    # Optional task attributes.
    task_params = task_request["params"]
    task_inputs = task_request["inputs"]
    task_inputs_names = [inn.split(".")[0] for inn in task_inputs]

    task_secrets = task_request["secrets"]
    task_unsealed_secrets = []

    for secret in task_secrets:
        task_unsealed_secrets.append(TaskSecret(**secret).unseal(settings.SECRETS_SK_KEY.get_secret_value()))

    # To this point, we can check if the task can be executed or not
    # i.e., if any input tasks are in pending state then there's no reason
    #   to execute this task yet.
    # -- start fix
    db = get_db_connection()
    are_pending = []

    time.sleep(5)

    tasks = TaskManager(db).find(parent=workflow_id)
    if not tasks:
        # Very strange, but we have no tasks (¿TODO?)
        raise ValueError(f"Tasks for workflow `{workflow_id}` not found")

    for task in tasks:
        if task in task_inputs_names:
            task_with_status = TaskManager(db).find_one(id=task_id)
            are_pending.append(task_with_status["status"] == TaskStatus.STATUS_PENDING)

    if any(are_pending):
        # Enqueue task again to be executed later.
        dramatiq.get_broker().enqueue(message)
        return
    # -- end fix

    # Task options.
    task_opts = task_request["options"]
    force_interruption = task_opts["on_fail_force_interruption"]
    remove_local_dir = task_opts["on_fail_remove_local_dir"]

    # Configure data file storage.
    storage = get_available_storage()
    dfs = storage(bucket_name=task_author, folder_name=[workflow_id, task_name])
    print("dfs")

    # Create process.
    task_process = Process(
        name=task_name,
        module=task_module,
        parent=workflow_id,
        params=task_params,
        inputs=task_inputs,
        secrets=task_unsealed_secrets,
        storage=dfs,
    )

    task_process.debug(f"Running task {task_id} with name {task_name}")

    try:
        # Import `execute` function from module.
        task_process.debug(f"Importing function from {task_module}")
        func = get_process_func(task_module)

        # Set process status to `running`.
        set_running(message)

        # Execute imported function.
        task_result = func(**task_params, pcs=task_process)
        if task_result:
            if not isinstance(task_result, TaskResult):
                task_result = TaskResult(message=str(task_result))
        if not task_result:
            task_result = TaskResult()
    except ImportError:
        task_process.error(traceback.format_exc())
        task_process.close(force_interruption=force_interruption)
        raise ImportError(f"Module {task_module} from task {task_id} is not available")
    except StopIteration:
        task_process.error(traceback.format_exc())
        task_process.close(force_interruption=force_interruption)
        raise StopIteration("Could not get data from upstream")
    except Exception:
        task_process.error("Unexpected unknown exception was raised by actor:")
        task_process.error(traceback.format_exc())
        task_process.close(force_interruption=force_interruption, remove_local_dir=remove_local_dir)
        raise

    # Append logging file to task result.
    remote_logging_file = task_process.close()
    task_process.info(f"Task {task_id} successfully executed")

    task_result.log = remote_logging_file

    # Result of this function *must be* JSON-encodable.
    data_as_json = task_result.json()

    # Set process status to `success`.
    set_success(task_id, data_as_json)

    return data_as_json


def set_workflow_run_state(workflow_id: str):
    """
    Sets workflow run status.
    TODO Global lock to update status.
    """
    db = get_db_connection()

    workflow = WorkflowManager(db).find_one(id=workflow_id)
    tasks = TaskManager(db).find(parent=workflow_id)

    tasks_statuses_only = []
    for task in tasks:
        status = task.status.upper()
        tasks_statuses_only.append(status)

    # Append global status based on task statuses.
    def _check(comp: Callable, stats: list) -> bool:
        return comp([s in stats for s in tasks_statuses_only])

    if workflow.is_revoked:
        workflow_status = WorkflowStatus.STATUS_REVOKED
    elif _check(all, [TaskStatus.STATUS_DONE]):
        workflow_status = WorkflowStatus.STATUS_DONE
    elif _check(any, [TaskStatus.STATUS_FAILED]):
        workflow_status = WorkflowStatus.STATUS_FAILED
    elif _check(all, [TaskStatus.STATUS_PENDING]):
        workflow_status = WorkflowStatus.STATUS_PENDING
    elif _check(any, [TaskStatus.STATUS_PENDING]) and not _check(any, [TaskStatus.STATUS_FAILED]):
        workflow_status = WorkflowStatus.STATUS_PENDING
    elif _check(any, [TaskStatus.STATUS_RUNNING]) and not _check(any, [TaskStatus.STATUS_FAILED]):
        workflow_status = WorkflowStatus.STATUS_RUNNING
    else:
        workflow_status = WorkflowStatus.STATUS_UNKNOWN

    WorkflowManager(db).create_or_update_from_id(
        workflow_id=workflow_id, updated_at=datetime.now(), status=workflow_status
    )


def set_running(message: MessageProxy):
    """
    Sets task status to `RUNNING`.
    """
    db = get_db_connection()

    task = TaskManager(db).create_or_update_from_id(
        message.message_id,
        status=TaskStatus.STATUS_RUNNING,
        updated_at=datetime.now(),
    )

    task_with_status = TaskManager(db).find_one(id=task.id)
    set_workflow_run_state(workflow_id=task_with_status.parent)


def set_success(message_id: str, result_data: str):
    """
    Actor success callback. Set task status to `DONE` and append result.
    """
    db = get_db_connection()
    task_result = TaskResult.parse_raw(result_data)

    task = TaskManager(db).create_or_update_from_id(
        message_id,
        status=TaskStatus.STATUS_DONE,
        updated_at=datetime.now(),
        result=task_result,
    )

    task_with_status = TaskManager(db).find_one(id=task.id)
    set_workflow_run_state(workflow_id=task_with_status.parent)


@dramatiq.actor(queue_name=settings.DEFAULT_ACTOR_OPTS.queue_name)
def set_failure(message, exception_data):
    """
    Actor failure callback. Set task status to `FAILED` and append traceback.
    """
    db = get_db_connection()
    task_result = TaskResult(message=exception_data)

    task = TaskManager(db).create_or_update_from_id(
        message["message_id"],
        status=TaskStatus.STATUS_FAILED,
        updated_at=datetime.now(),
        result=task_result,
    )

    task_with_status = TaskManager(db).find_one(id=task.id)
    set_workflow_run_state(workflow_id=task_with_status.parent)
