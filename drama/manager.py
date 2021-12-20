from abc import ABC
from typing import List, Optional, Tuple

from pymongo.database import Database

from drama.database import get_db_connection
from drama.models.workflow import TaskInDb, WorkflowInDb


class _BaseManager(ABC):
    """
    Base manager with database connection.
    """

    def __init__(self, db: Optional[Database] = None):
        self.database = db or get_db_connection()


class TaskManager(_BaseManager):
    def find(self, **query):
        """
        Get task(s) from database based on `query`.
        """
        tasks: dict = self.database.dramatask.find(query)
        tasks_in_db = []

        for task in tasks:
            tasks_in_db.append(TaskInDb(**task))

        return tasks_in_db

    def find_one(self, **query):
        """
        Get task from database based on `query`.
        """
        task_in_db: dict = self.database.dramatask.find_one(query)

        if task_in_db:
            return TaskInDb(**task_in_db)

    def create_or_update_from_id(self, task_id: str, **extra_fields) -> TaskInDb:
        """
        Create (or update with `extra_fields`) task from database based on unique `task_id`.
        """
        task = TaskInDb(id=task_id, **extra_fields)

        # if the record does not exist, insert it
        self.database.dramatask.update(
            {"id": task_id},
            {"$set": task.dict(exclude_unset=True)},
            upsert=True,
        )

        return task


class WorkflowManager(_BaseManager):
    def find_one(self, **query) -> Optional[WorkflowInDb]:
        """
        Get workflow from database based on `query`.
        """
        workflow_in_db: dict = self.database.dramaworkflow.find_one(query)

        if workflow_in_db:
            return WorkflowInDb(**workflow_in_db)

        return None

    def create_or_update_from_id(self, workflow_id: str, **extra_fields) -> WorkflowInDb:
        """
        Create (or update with `extra_fields`) workflow from database based on unique `workflow_id`.
        """
        workflow = WorkflowInDb(id=workflow_id, **extra_fields)

        # if the record does not exist, insert it
        self.database.dramaworkflow.update(
            {"id": workflow_id},
            {"$set": workflow.dict(exclude_unset=True)},
            upsert=True,
        )

        return workflow
