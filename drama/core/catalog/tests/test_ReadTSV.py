import shutil
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from drama.core.catalog.read.ReadTSV import execute
from drama.core.catalog.tests import RESOURCES
from drama.core.model import SimpleTabularDataset
from drama.storage import LocalStorage


class ReadTSVIntegrationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "tests", "test_ReadTSV"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        dataset_csv = shutil.copy(Path(RESOURCES, "countries.csv"), storage.local_dir)

        # mock process
        self.pcs = MagicMock(storage=storage)
        self.pcs.get_from_upstream = MagicMock(
            return_value={"TabularDataset": [{"resource": dataset_csv, "delimiter": ","}]}
        )

    def test_integration(self):
        # execute func
        execute(pcs=self.pcs)

        # assert input files exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "countries.csv").is_file())

    def tearDown(self) -> None:
        self.pcs.storage.on_fail_remove_local_dir()


class ReadTSVComponentTestCase(unittest.TestCase):
    def test_definition(self):
        self.assertEqual(getattr(execute, "inputs"), ("TabularDataset", SimpleTabularDataset))
        self.assertEqual(getattr(execute, "outputs"), None)


if __name__ == "__main__":
    unittest.main()
