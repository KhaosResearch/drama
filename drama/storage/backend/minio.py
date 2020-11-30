import json
import shutil
from pathlib import Path
from typing import Optional, Union

from filelock import FileLock
from minio import Minio, ResponseError
from minio.error import BucketAlreadyOwnedByYou

from drama.config import settings
from drama.logger import get_logger
from drama.storage.base import Storage

logger = get_logger(__name__)


class MinIOStorage(Storage):
    def __init__(self, bucket_name: str, folder_name: str):
        super().__init__(bucket_name, folder_name)
        self.client = Minio(
            settings.MINIO_CONN,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=False,
        )

    def setup(self) -> str:
        super().setup()

        policy_read_only = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": {"AWS": "*"},
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{self.bucket_name}/*",
                }
            ],
        }

        try:
            self.client.make_bucket(self.bucket_name)
            self.client.set_bucket_policy(self.bucket_name, json.dumps(policy_read_only))
        except BucketAlreadyOwnedByYou:
            pass

        return f"minio:/{self.bucket_name}/"

    def put_file(self, file_path: Union[str, Path], rename: Optional[str] = None) -> str:
        if isinstance(file_path, Path):
            file_path = str(file_path)

        file_name = Path(file_path).name if not rename else rename

        # copy file from task to current task directory
        if not file_path.startswith(str(self.local_dir)):
            file_path = shutil.copy(file_path, Path(self.local_dir, file_name))
        object_name = str(Path(self.folder_name, file_name))

        try:
            self.client.fput_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                file_path=file_path,
            )
        except ResponseError as err:
            logger.error(f"Could not put file {object_name} from {self.bucket_name}")
            logger.exception(err)
            raise

        return f"minio:/{self.bucket_name}/{self.folder_name}/{file_name}"

    def get_file(self, data_file: str) -> str:
        if not data_file.startswith("minio:"):
            raise Exception("Object file prefix is invalid: expected `minio:`")

        _, bucket_name, folder_name, file_name = data_file.split("/")

        file_path = Path(self.temp_dir, bucket_name, folder_name, file_name)
        file_lock = FileLock(str(file_path) + ".lock")  # avoid race: https://github.com/minio/minio-py/issues/854

        object_name = str(Path(folder_name, file_name))

        with file_lock:
            if not file_path.is_file():
                try:
                    self.client.fget_object(
                        bucket_name=bucket_name,
                        object_name=object_name,
                        file_path=(str(file_path)),
                    )
                except ResponseError as err:
                    logger.error(f"Could not get file {object_name} from {self.bucket_name}")
                    logger.exception(err)
                    raise

        return str(file_path)
