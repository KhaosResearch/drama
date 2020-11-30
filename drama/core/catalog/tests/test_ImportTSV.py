import shutil
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

from drama.core.catalog.load.ImportTSV import execute
from drama.core.catalog.tests import RESOURCES
from drama.core.model import SimpleTabularDataset
from drama.storage import LocalStorage


def _mocked_urlretrieve(url, filepath):
    shutil.copy(Path(RESOURCES, "param.tsv"), filepath)


class ImportTSVIntegrationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "tests", "test_ImportTSV"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        self.pcs = MagicMock(storage=storage)

    def test_definition(self):
        self.assertEqual(getattr(execute, "inputs"), None)
        self.assertEqual(getattr(execute, "outputs"), SimpleTabularDataset)

    @mock.patch("urllib.request.urlretrieve", side_effect=_mocked_urlretrieve)
    def test_integration(self, urlretrieve):
        # execute func
        url = "http://testsite.com/param.tsv"
        data = execute(pcs=self.pcs, url=url)

        with Path(self.pcs.storage.local_dir, "out.tsv").open() as fin:
            out_tsv = fin.read()

        # assert output file exists
        self.assertTrue(Path(self.pcs.storage.local_dir, "out.tsv").is_file())

        # assert output file content is valid
        self.assertMultiLineEqual("param1  False\nparam2	Yes\nparam3\n", out_tsv)

        # assert output data is valid
        self.assertEqual(data.keys(), {"output", "resource"})
        self.assertIs(type(data["output"]), SimpleTabularDataset)

    def tearDown(self) -> None:
        self.pcs.storage.on_fail_remove_local_dir()


class ImportFileComponentTestCase(unittest.TestCase):
    def test_definition(self):
        self.assertEqual(getattr(execute, "inputs"), None)
        self.assertEqual(getattr(execute, "outputs"), SimpleTabularDataset)


if __name__ == "__main__":
    unittest.main()
