import os
import unittest
from pathlib import Path

from drama.storage import LocalStorage

ABS_DIRNAME = os.path.dirname(os.path.abspath(__file__))


class LocalStorageTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_parent, task_name = "tests", "test_storage"

        self.storage = LocalStorage(bucket_name=task_parent, folder_name=task_name)
        self.storage.setup()

    def test_storage_creates_valid_directory(self):
        self.assertTrue(self.storage.local_dir.is_dir())

    def test_file_can_be_put(self):
        curr_file = Path(ABS_DIRNAME, "test_storage.py")
        _ = self.storage.put_file(curr_file)

        self.assertTrue(Path(self.storage.local_dir, "test_storage.py").is_file())

    def test_file_can_be_put_and_renamed(self):
        curr_file = Path(ABS_DIRNAME, "test_storage.py")
        self.storage.put_file(curr_file, rename="storage_test.py")

        self.assertTrue(Path(self.storage.local_dir, "storage_test.py").is_file())

    def test_raised_exception_when_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            self.storage.get_file("tests.py")

    def tearDown(self) -> None:
        self.storage.remove_local_dir()

        self.assertFalse(self.storage.local_dir.is_dir())


if __name__ == "__main__":
    unittest.main()
