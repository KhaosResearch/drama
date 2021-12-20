import os
import unittest
from pathlib import Path

from pydantic.error_wrappers import ValidationError

from drama.storage import LocalStorage
from drama.storage.backend.local import LocalResource
from drama.storage.base import Resource

ABS_DIRNAME = os.path.dirname(os.path.abspath(__file__))


class BaseStorageModelTestCase(unittest.TestCase):
    def test_invalid_resource_with_no_scheme_nor_resource(self):
        with self.assertRaises(ValidationError):
            Resource()

    def test_invalid_resource_with_no_resource(self):
        with self.assertRaises(ValidationError):
            Resource(scheme="test://")

    def test_resource_can_be_encoded(self):
        resource = Resource(resource="hello.txt", scheme="")
        self.assertEqual(resource.encode(), b"hello.txt")


class LocalStorageTestCase(unittest.TestCase):
    def setUp(self) -> None:
        task_parent, task_name = "tests", "test_storage"

        self.storage = LocalStorage(bucket_name=task_parent, folder_name=[task_name])
        self.storage.setup()

    def test_storage_creates_valid_directory(self):
        self.assertTrue(self.storage.local_dir.is_dir())

    def test_file_can_be_put(self):
        curr_file = Path(ABS_DIRNAME, "test_storage.py")
        resource = self.storage.put_file(curr_file)

        self.assertTrue(Path(self.storage.local_dir, "test_storage.py").is_file())
        self.assertIs(type(resource), LocalResource)

    def test_file_can_be_put_and_renamed(self):
        curr_file = Path(ABS_DIRNAME, "test_storage.py")
        _ = self.storage.put_file(curr_file, rename="storage_test.py")

        self.assertTrue(Path(self.storage.local_dir, "storage_test.py").is_file())

    def test_raised_exception_when_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            self.storage.get_file("tests.py")

    def tearDown(self) -> None:
        self.storage.remove_local_dir()

        self.assertFalse(self.storage.local_dir.is_dir())


if __name__ == "__main__":
    unittest.main()
