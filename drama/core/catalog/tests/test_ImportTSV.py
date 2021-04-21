import shutil
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock

from drama.core.annotation import TaskMeta
from drama.core.catalog.load.ImportTSV import execute
from drama.core.catalog.tests import RESOURCES
from drama.core.model import SimpleTabularDataset
from drama.models.task import TaskResult
from drama.storage import LocalStorage


def _mocked_urlretrieve(url, filepath):
    shutil.copy(Path(RESOURCES, "param.tsv"), filepath)


class ImportTSVIntegrationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_id, task_name = "tests", "test_ImportTSV"

        storage = LocalStorage(bucket_name=task_id, folder_name=task_name)
        storage.setup()

        self.pcs = MagicMock(storage=storage)

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
        self.assertIs(type(data), TaskResult)

    def tearDown(self) -> None:
        self.pcs.storage.remove_local_dir()


class ImportFileComponentTestCase(unittest.TestCase):
    def test_definition(self):
        meta: TaskMeta = getattr(execute, "__meta__")
        self.assertEqual(meta.inputs, None)
        self.assertEqual(meta.outputs, SimpleTabularDataset)


if __name__ == "__main__":
    unittest.main()
