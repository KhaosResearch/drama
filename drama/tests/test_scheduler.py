import unittest

from drama.models.task import Task
from drama.models.workflow import Workflow
from drama.worker.scheduler import Scheduler


class SchedulerTestCase(unittest.TestCase):
    def test_should_compute_dag_from_workflow(self) -> None:
        task_one = Task(
            name="First",
            module="test",
        )

        task_two = Task(
            name="Second",
            module="test",
            inputs={
                "Input": "First.Data",
            },
        )

        task_three = Task(
            name="Third",
            module="test",
            inputs={
                "Input": "First.Data",
            },
        )

        workflow = Workflow(tasks=[task_one, task_two, task_three])

        self.assertEqual(Scheduler.sorted_tasks(workflow), ["First", "Second", "Third"])

    def test_should_compute_dag_from_workflow_case_b(self) -> None:
        task_one = Task(
            name="First",
            module="test",
        )

        task_two = Task(
            name="Second",
            module="test",
            inputs={
                "Input": "First.Data",
            },
        )

        task_three = Task(
            name="Three",
            module="test",
            inputs={
                "Input": "First.Data",
            },
        )

        task_four = Task(
            name="Fourth",
            module="test",
        )

        workflow = Workflow(tasks=[task_one, task_two, task_three, task_four])

        self.assertEqual(["First", "Second", "Three", "Fourth"], Scheduler.sorted_tasks(workflow))

    def test_should_compute_dag_from_workflow_case_c(self) -> None:
        task_one = Task(
            name="First",
            module="test",
        )

        task_two = Task(
            name="Second",
            module="test",
        )

        task_three = Task(
            name="Third",
            module="test",
            inputs={
                "Input": "First.Data",
            },
        )

        task_four = Task(
            name="Fourth",
            module="test",
            inputs={
                "Input": "First.Data",
            },
        )

        task_five = Task(
            name="Fifth",
            module="test",
            inputs={
                "Input": "Third.Data",
            },
        )

        task_six = Task(
            name="Sixth",
            module="test",
            inputs={
                "Input": "Fourth.Data",
            },
        )

        task_seven = Task(
            name="Seventh",
            module="test",
            inputs={
                "Input": "Fourth.Data",
            },
        )

        workflow = Workflow(tasks=[task_one, task_two, task_three, task_four, task_five, task_six, task_seven])

        self.assertEqual(
            ["First", "Third", "Fifth", "Fourth", "Sixth", "Seventh", "Second"], Scheduler.sorted_tasks(workflow)
        )

    def test_should_compute_dag_from_workflow_case_d(self) -> None:
        task_one = Task(
            name="ComponentImportFile0",
            module="test",
        )

        task_two = Task(
            name="ComponentImportFile1",
            module="test",
        )

        task_three = Task(
            name="ComponentTrophPos0",
            module="test",
            inputs={
                "Input": "ComponentImportFile0.Data",
            },
        )

        task_four = Task(
            name="ComponentShapeFileCreator0",
            module="test",
            inputs={
                "Input": "ComponentTrophPos0.Data",
            },
        )

        task_five = Task(
            name="ComponentSpatialViewer0",
            module="test",
            inputs={
                "Input1": "ComponentTrophPos0.Data",
                "Input2": "ComponentShapeFileCreator0.Data",
            },
        )

        task_six = Task(
            name="ComponentCopernicusLink0",
            module="test",
            inputs={
                "Input0": "ComponentTrophPos0.Data",
                "Input1": "ComponentSpatialViewer0.Data",
            },
        )

        task_seven = Task(
            name="ComponentModeler0",
            module="test",
            inputs={
                "Input0": "ComponentTrophPos0.Data",
                "Input1": "ComponentCopernicusLink0.Data",
                "Input2": "ComponentImportFile1.Data",
            },
        )

        workflow = Workflow(tasks=[task_one, task_two, task_three, task_four, task_five, task_six, task_seven])

        self.assertEqual(
            [
                "ComponentImportFile0",
                "ComponentTrophPos0",
                "ComponentShapeFileCreator0",
                "ComponentSpatialViewer0",
                "ComponentCopernicusLink0",
                "ComponentImportFile1",
                "ComponentModeler0",
            ],
            Scheduler.sorted_tasks(workflow),
        )


if __name__ == "__main__":
    unittest.main()
