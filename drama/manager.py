from abc import ABC
from typing import Optional

from pymongo.database import Database

from drama.database import get_db_connection
from drama.models.workflow import Task, Workflow


class BaseManager(ABC):
    """
    Base manager with database connection.
    """

    def __init__(self, db: Optional[Database] = None):
        self.database = db or get_db_connection()


class TaskManager(BaseManager):
    def find(self, query: dict):
        """
        Get task(s) from database based on `query`.
        """
        tasks: dict = self.database.task.find(query)
        tasks_in_db = []

        for task in tasks:
            tasks_in_db.append(Task(**task))

        return tasks_in_db

    def create_or_update_from_id(self, task_id: str, **extra_fields) -> Task:
        """
        Create (or update with `extra_fields`) task from database based on unique `task_id`.
        """
        task = Task(id=task_id, **extra_fields)

        # if the record does not exist, insert it
        self.database.task.update(
            {"id": task_id},
            {"$set": task.dict(exclude_unset=True)},
            upsert=True,
        )

        return task


class WorkflowManager(BaseManager):
    def find_one(self, query: dict) -> Optional[Workflow]:
        """
        Get workflow from database based on `query`.
        """
        workflow_in_db: dict = self.database.workflow.find_one(query)

        if workflow_in_db:
            return Workflow(**workflow_in_db)

        return None

    def create_or_update_from_id(self, workflow_id: str, **extra_fields) -> Workflow:
        """
        Create (or update with `extra_fields`) workflow from database based on unique `workflow_id`.
        """
        workflow = Workflow(id=workflow_id, **extra_fields)

        # if the record does not exist, insert it
        self.database.workflow.update(
            {"id": workflow_id},
            {"$set": workflow.dict(exclude_unset=True)},
            upsert=True,
        )

        return workflow
