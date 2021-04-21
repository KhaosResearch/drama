import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Union

from pydantic import BaseModel, validator

from drama.config import settings
from drama.logger import get_logger

logger = get_logger(__name__)


class NotValidScheme(Exception):
    """
    Raised when scheme is not present.
    """

    pass


class Resource(BaseModel):
    scheme: str = ""
    resource: str

    @validator("resource")
    def resource_must_have_scheme(cls, v, values, **kwargs):
        assert str(v).startswith(values.get("scheme", "")), "invalid resource"
        return v

    def encode(self, **kwargs):
        """
        This is required for fastavro writer.
        """
        return self.resource.encode(**kwargs)


class LocalResource(Resource):
    pass


class Storage(ABC):
    def __init__(self, bucket_name: str, folder_name: str):
        self.bucket_name = bucket_name
        self.folder_name = folder_name

        self.temp_dir = settings.DATA_DIR
        self.local_dir = Path(self.temp_dir, self.bucket_name, self.folder_name)

    def setup(self) -> LocalResource:
        """
        Setup directory tree.
        """
        self.local_dir.mkdir(parents=True, exist_ok=True)
        return LocalResource(resource=str(self.local_dir))

    @abstractmethod
    def put_file(self, file_path: Union[str, Path], rename: Optional[str] = None) -> Resource:
        """
        Uploads an object to storage and returns its data file identifier.
        """
        pass

    @abstractmethod
    def get_file(self, data_file: str) -> str:
        """
        Downloads an object from storage to `task_dir` (if it's *NOT* already on disk).
        """
        pass

    def remove_local_dir(self, omit_files: List[str] = None) -> None:
        """
        Remove `task_dir` local directory from system.
        If `omit_files`, remove `task_dir` directory's content from system instead while keeping some files.

        :param omit_files: List of filenames to keep.
        """
        if omit_files is None:
            omit_files = []

        logger.warning(f"Directory at {self.local_dir} is being deleted from local filesystem")

        for item in os.listdir(self.local_dir):
            item_path = Path(self.local_dir, item)

            if item in omit_files:
                shutil.move(str(item_path), str(Path(self.local_dir, f"{item}.old")))
                continue

            logger.warning(f"Item {item_path} marked for remove")

            if item_path.is_dir():
                shutil.rmtree(item_path, ignore_errors=True)
            else:
                os.remove(item_path)

        if not omit_files:
            shutil.rmtree(self.local_dir, ignore_errors=True)

    @abstractmethod
    def remove_remote_dir(self, omit_files: List[str] = None) -> None:
        """
        Remove `task_dir` directory from remote storage.
        If `omit_files`, remove `task_dir` directory's content from remote storage instead while keeping some files.

        :param omit_files: List of filenames to keep.
        """
        pass
