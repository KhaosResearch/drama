import shutil
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

from drama.core.catalog.load.ImportFile import execute
from drama.core.catalog.tests import RESOURCES
from drama.core.model import TempFile
from drama.models.task import TaskResult
from drama.storage import LocalStorage


class ImportFileIntegrationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "tests", "test_ImportFile"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        # copy file to task dir
        _ = shutil.copy(Path(RESOURCES, "countries.csv"), storage.local_dir)

        self.pcs = MagicMock(storage=storage)
        self.pcs.to_downstream = MagicMock(return_value=None)

    @mock.patch("urllib.request.urlretrieve")
    def test_integration(self, urlretrieve):
        # execute func
        url = "https://raw.githubusercontent.com/cs109/2014_data/master/countries.csv"
        data = execute(pcs=self.pcs, url=url)

        # assert output file exist
        self.assertTrue(Path(self.pcs.storage.local_dir, "countries.csv").is_file())

        # assert output file content is valid
        with open(Path(self.pcs.storage.local_dir, "countries.csv"), "r") as f:
            columns = f.readline()
            self.assertEqual(columns, "Country,Region\n")

        # assert output is valid
        self.assertIs(type(data), TaskResult)

    def tearDown(self) -> None:
        self.pcs.storage.remove_local_dir()


class ImportFileComponentTestCase(unittest.TestCase):
    def test_definition(self):
        meta = getattr(execute, "__meta__")

        self.assertEqual(meta.inputs, None)
        self.assertEqual(meta.outputs, TempFile)


if __name__ == "__main__":
    unittest.main()
