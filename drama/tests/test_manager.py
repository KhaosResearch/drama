import unittest

from drama.manager import TaskManager, WorkflowManager
from drama.models.workflow import TaskInDb, WorkflowInDb, WorkflowStatus


class _MongoClient:
    """
    Mock MongoClient for testing pourposes.
    """

    def __init__(self):
        self.collection = []

    def find(self, query: dict, **kwargs):
        """
        Find documents from collection based on query.
        """
        docs = []
        for doc in self.collection:
            if self._is_subset(query, doc):
                docs.append(doc)

        return docs

    def find_one(self, query: dict, **kwargs):
        """
        Find document from collection based on query.
        """
        for doc in self.collection:
            if self._is_subset(query, doc):
                return doc

    def update(self, query: dict, update: dict, **kwargs):
        """
        Create or update (upsert) document to collection.
        """
        for doc in self.collection:
            if self._is_subset(query, doc):
                doc.update(update["$set"])
                break

    @staticmethod
    def _is_subset(a: dict, b: dict) -> bool:
        """
        Returns if `a` items are contained in `b`.
        """
        return all(item in b.items() for item in a.items())

    def __getattr__(self, item):
        return self


class TaskManagerTestCase(unittest.TestCase):
    def test_should_get_tasks_from_parent_id(self):
        task = TaskInDb(id="task_id", parent="parent_id")

        db = _MongoClient()
        db.collection = [
            task.dict(),
        ]
        manager = TaskManager(db=db)

        self.assertEqual([task], manager.find(parent="parent_id"))

    def test_should_get_task_from_id(self):
        task = TaskInDb(id="task_id")

        db = _MongoClient()
        db.collection = [
            task.dict(),
        ]
        manager = TaskManager(db=db)

        self.assertEqual([task], manager.find(id="task_id"))

    def test_should_not_get_missing_task_from_id(self):
        db = _MongoClient()
        manager = TaskManager(db=db)

        self.assertEqual([], manager.find(id="task_id"))

    def test_should_create_new_task_from_id_with_default_values(self):
        db = _MongoClient()
        manager = TaskManager(db=db)

        task = manager.create_or_update_from_id("new_id")

        self.assertEqual(task, TaskInDb(id="new_id").dict())

    def test_should_update_task(self):
        db = _MongoClient()
        manager = TaskManager(db=db)

        old_task = manager.create_or_update_from_id("new_id", name="my_task", module="new_module")
        new_task = manager.create_or_update_from_id("new_id", name="new_task_name", module="new_module")

        self.assertEqual(old_task, TaskInDb(id="new_id", name="my_task", module="new_module").dict())
        self.assertEqual(new_task, TaskInDb(id="new_id", name="new_task_name", module="new_module").dict())


class WorkflowManagerTestCase(unittest.TestCase):
    def test_should_get_workflow_from_id(self):
        w1 = WorkflowInDb(id="workflow_id")

        db = _MongoClient()
        db.collection = [
            w1.dict(),
        ]
        manager = WorkflowManager(db=db)

        self.assertEqual(w1, manager.find_one(id="workflow_id"))

    def test_should_create_new_workflow_from_id_with_default_values(self):
        db = _MongoClient()
        manager = WorkflowManager(db=db)

        workflow = manager.create_or_update_from_id("new_id")

        self.assertEqual(workflow, WorkflowInDb(id="new_id").dict())

    def test_should_update_workflow(self):
        db = _MongoClient()
        manager = WorkflowManager(db=db)

        old_workflow = manager.create_or_update_from_id("new_id", status=WorkflowStatus.STATUS_PENDING)
        new_workflow = manager.create_or_update_from_id("new_id", status=WorkflowStatus.STATUS_RUNNING)

        self.assertEqual(old_workflow, WorkflowInDb(id="new_id", status=WorkflowStatus.STATUS_PENDING).dict())
        self.assertEqual(new_workflow, WorkflowInDb(id="new_id", status=WorkflowStatus.STATUS_RUNNING).dict())


if __name__ == "__main__":
    unittest.main()
