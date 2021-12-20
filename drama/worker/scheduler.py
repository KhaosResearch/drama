from collections import defaultdict
from datetime import datetime

import dramatiq

from drama.config import settings
from drama.database import DataBase, get_db_connection
from drama.logger import get_logger
from drama.manager import TaskManager, WorkflowManager
from drama.models.task import Task, TaskInDb, TaskStatus
from drama.models.workflow import Workflow, WorkflowInDb, WorkflowStatus
from drama.worker import set_failure, worker

logger = get_logger(__name__)


class Scheduler:
    def __init__(self, db: DataBase = None):
        self.db = db or get_db_connection()

    def run(self, workflow: Workflow) -> WorkflowInDb:
        """
        Send workflow request to main `drama` actor.
        Workflow is divided into individual tasks and processed by the former actor.
        """
        # Creates workflow in database.
        workflow_in_db = WorkflowManager(self.db).create_or_update_from_id(
            workflow.id,
            labels=workflow.labels,
            metadata=workflow.metadata,
            created_at=datetime.now(),
            status=WorkflowStatus.STATUS_PENDING,
        )

        tasks = {}
        for task in workflow.tasks:
            # Update task to include workflow metadata.
            task.metadata.update(workflow.metadata)
            tasks[task.name] = task

        # Execute workflow in order.
        sorted_tasks = self.sorted_tasks(workflow)
        for task_name in sorted_tasks:
            self.enqueue(task_request=tasks[task_name], workflow_id=workflow.id)

        return workflow_in_db

    def revoke(self, workflow_id: str) -> WorkflowInDb:
        """
        Cancel workflow execution.
        """
        logger.debug(f"Revoking workflow id {workflow_id}")

        # Updates workflow in database
        workflow = WorkflowManager().create_or_update_from_id(workflow_id, updated_at=datetime.now(), is_revoked=True)

        # Executes a new task to revoke workflow
        task_revoke = Task(
            name="RevokeExecution",
            module="drama.core.utils.RevokeExecution",
        )
        self.enqueue(task_revoke, workflow_id=workflow_id)

        return workflow

    def enqueue(self, task_request: Task, workflow_id: str) -> TaskInDb:
        """
        Send task request to main `drama` actor.
        """
        # Triggers actor execution
        task_dict = task_request.dict()
        _message = worker.message_with_options(
            args=(task_dict, workflow_id),
            on_failure=set_failure,
        )
        _message = _message.copy(
            queue_name=task_request.options.queue_name or settings.DEFAULT_ACTOR_OPTS.queue_name,
        )

        broker = dramatiq.get_broker()
        message = broker.enqueue(_message)

        # Creates task on database
        task = TaskManager(self.db).create_or_update_from_id(
            message.message_id,
            name=task_request.name,
            parent=workflow_id,
            module=task_request.module,
            params=task_request.params,
            inputs=task_request.inputs,
            labels=task_request.labels,
            options=task_request.options,
            metadata=task_request.metadata,
            status=TaskStatus.STATUS_PENDING,
            created_at=datetime.now(),
        )

        return task

    def status(self, workflow_id: str) -> WorkflowInDb:
        workflow = WorkflowManager(self.db).find_one(id=workflow_id)
        workflow.tasks = TaskManager(self.db).find(parent=workflow_id)

        return workflow

    @staticmethod
    def sorted_tasks(workflow: Workflow) -> list:
        def iterative_topological_sort(graph, start):
            seen = set()
            stack = []  # path variable is gone, stack and order are new
            order = []  # order will be in reverse order at first
            q = [s for s in start]
            while q:
                v = q.pop()
                if v not in seen:
                    seen.add(v)  # no need to append to path any more
                    q.extend(graph[v])

                    while stack and v not in graph[stack[-1]]:  # new stuff here!
                        order.append(stack.pop())
                    stack.append(v)

            return stack + order[::-1]  # new return value!

        sources = []
        graph = defaultdict(list)

        for task in workflow.tasks:
            if not task.inputs:
                sources.append(task.name)
            else:
                for _, task_input in task.inputs.items():
                    graph[task_input.split(".")[0]].append(task.name)

        ordered_workflow_tasks = iterative_topological_sort(graph, sources)

        return ordered_workflow_tasks

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
