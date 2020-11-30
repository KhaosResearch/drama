import unittest

from pydantic import ValidationError

from drama.models.task import TaskRequest
from drama.models.workflow import WorkflowRequest


class TaskModelTestCase(unittest.TestCase):
    def test_should_create_valid_task_request(self) -> None:
        task = TaskRequest(name="test_name", module="test_module")

        self.assertEqual(task.name, "test_name")
        self.assertEqual(task.module, "test_module")

    def test_should_raise_validation_error(self) -> None:
        with self.assertRaises(ValidationError):
            TaskRequest()

    def test_should_not_raise_validation_error_with_correct_inputs(self) -> None:
        TaskRequest(name="test_name", module="test_module", inputs={"in1": "test_name_2.out", "in2": "test_name_3.out"})

    def test_should_raise_validation_error_with_incorrect_inputs(self) -> None:
        with self.assertRaises(ValidationError):
            TaskRequest(name="test_name", module="test_module", inputs={"in1": "test_name_2.out", "in2": "test_name_3"})


class WorkflowModelTestCase(unittest.TestCase):
    def test_should_create_valid_workflow_request(self) -> None:
        task_one = TaskRequest(name="test_name_one", module="test_module_one")
        task_two = TaskRequest(name="test_name_two", module="test_module_two")

        WorkflowRequest(tasks=[task_one, task_two])

    def test_should_raise_validation_error(self) -> None:
        task_one = {}

        with self.assertRaises(ValidationError):
            WorkflowRequest(tasks=[task_one])


if __name__ == "__main__":
    unittest.main()
