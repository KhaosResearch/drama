import unittest

from pydantic import ValidationError

from drama.models.task import Task, TaskSecret
from drama.models.workflow import Workflow


class TaskModelTestCase(unittest.TestCase):
    def test_should_create_valid_task_request(self) -> None:
        task = Task(name="test_name", module="test_module")

        self.assertEqual(task.name, "test_name")
        self.assertEqual(task.module, "test_module")

    def test_should_create_valid_task_request_with_secrets(self) -> None:
        task = Task(
            name="test_name", module="test_module", secrets=[{"token": "MY_SECRET", "secret": b"Kill all kittens"}]
        )

    def test_should_raise_validation_error(self) -> None:
        with self.assertRaises(ValidationError):
            Task()

    def test_should_not_raise_validation_error_with_correct_inputs(self) -> None:
        Task(name="test_name", module="test_module", inputs={"in1": "test_name_2.out", "in2": "test_name_3.out"})

    def test_should_raise_validation_error_with_incorrect_inputs(self) -> None:
        with self.assertRaises(ValidationError):
            Task(name="test_name", module="test_module", inputs={"in1": "test_name_2.out", "in2": "test_name_3"})

    def test_should_unseal_secrets(self) -> None:
        sk = "qmADWyCD3i4JJQkmyy/Y8YSPYi0/rO+x+rvMUM0Kxs8="
        st = "+aoJgK/BpkVGDldn9UBQ06RxG8eD2mgP/U4dZKmTpk3pyiTFfZhs8cE0e9jS8koxL21O2ZAwYNHjOMOgK/akBQ=="

        secret = TaskSecret(token="MY_SECRET", secret=st)
        decrypted = secret.unseal(sk)

        self.assertEqual(decrypted.secret, "Kill all kittens")


class WorkflowModelTestCase(unittest.TestCase):
    def test_should_create_valid_workflow_request(self) -> None:
        task_one = Task(name="test_name_one", module="test_module_one")
        task_two = Task(name="test_name_two", module="test_module_two")

        Workflow(tasks=[task_one, task_two])

    def test_should_raise_validation_error(self) -> None:
        task_one = {}

        with self.assertRaises(ValidationError):
            Workflow(tasks=[task_one])


if __name__ == "__main__":
    unittest.main()
