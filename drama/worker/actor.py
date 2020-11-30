import json
import traceback
from datetime import datetime

import dramatiq
from dramatiq import MessageProxy
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.middleware import CurrentMessage

from drama.config import settings
from drama.database import get_db_connection
from drama.logger import get_logger
from drama.manager import TaskManager
from drama.models.task import Result, TaskStatus
from drama.process import Process
from drama.storage.helpers import get_available_storage
from drama.worker.helpers import get_process_func

logger = get_logger(__name__)

# setup broker
logger.debug("Setting up RabbitMQ broker")
broker = RabbitmqBroker(url=settings.RABBIT_DNS)
broker.declare_queue(settings.DEFAULT_ACTOR_OPTS.queue_name)
broker.add_middleware(CurrentMessage())

# set broker
logger.debug("Attaching broker")
dramatiq.set_broker(broker)


@dramatiq.actor(**settings.DEFAULT_ACTOR_OPTS.dict())
def process(task_request: dict):
    """
    Main `drama` actor.
    Executes an arbitrary function defined by a task and updates its state.
    """
    message = CurrentMessage.get_current_message()
    task_id = message.message_id

    # required attributes
    task_name = task_request["name"]
    task_module = task_request["module"]
    task_parent = task_request["parent"]  # workflow id

    # optional attributes
    task_params = task_request["params"]
    task_inputs = task_request["inputs"]

    # task options
    task_opts = task_request["options"]
    force_interruption = task_opts["on_fail_force_interruption"]
    remove_local_dir = task_opts["on_fail_remove_local_dir"]

    # configure data file storage
    storage = get_available_storage()
    dfs = storage(bucket_name=task_parent, folder_name=task_name)  # bucket folder is shared across tasks in a workflow

    # create process
    task_process = Process(
        name=task_name,
        module=task_module,
        parent=task_parent,
        params=task_params,
        inputs=task_inputs,
        storage=dfs,
    )

    task_process.debug(f"Running task {task_id} with name {task_name}")

    try:
        # import `execute` function from module
        task_process.debug(f"Importing function from {task_module}")
        func = get_process_func(task_module)

        # set process status to `running`
        process_running(message)

        # execute imported function
        data = func(**task_params, pcs=task_process)

        # result *must be* JSON serializable
        if data and not isinstance(data, str):
            logger.debug(f"Result from task {task_id} not JSON serializable, trying auto conversion")
            if isinstance(data, dict):
                data = json.dumps(data, default=str)
            else:
                data = str(data)
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

    task_process.close()

    task_process.info(f"Task {task_id} successfully executed")

    return data


def process_running(message: MessageProxy):
    """
    Set task status to `RUNNING`.
    """
    db = get_db_connection()

    TaskManager(db).create_or_update_from_id(
        message.message_id,
        status=TaskStatus.STATUS_RUNNING,
        updated_at=datetime.now(),
    )


@dramatiq.actor(queue_name=settings.DEFAULT_ACTOR_OPTS.queue_name)
def process_succeeded(message, result_data):
    """
    Actor success callback. Set task status to `DONE` and append result.
    """
    db = get_db_connection()

    TaskManager(db).create_or_update_from_id(
        message["message_id"],
        status=TaskStatus.STATUS_DONE,
        updated_at=datetime.now(),
        result=Result(message=result_data),
    )


@dramatiq.actor(queue_name=settings.DEFAULT_ACTOR_OPTS.queue_name)
def process_failure(message, exception_data):
    """
    Actor failure callback. Set task status to `FAILED` and append traceback.
    """
    db = get_db_connection()

    TaskManager(db).create_or_update_from_id(
        message["message_id"],
        status=TaskStatus.STATUS_FAILED,
        updated_at=datetime.now(),
        result=Result(message=exception_data),
    )
