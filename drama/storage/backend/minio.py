import json
import shutil
from pathlib import Path
from typing import List, Optional, Union

from filelock import FileLock
from minio import Minio
from minio.error import S3Error

from drama.config import settings
from drama.logger import get_logger
from drama.storage.base import NotValidScheme, Resource, Storage

logger = get_logger(__name__)


class MinIOResource(Resource):
    scheme = "minio://"


class MinIOStorage(Storage):
    def __init__(self, bucket_name: str, folder_name: str):
        super().__init__(bucket_name, folder_name)
        self.client = Minio(
            settings.MINIO_CONN,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=False,
        )

    def setup(self) -> MinIOResource:
        super().setup()

        policy_read_only = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "",
                    "Effect": "Allow",  # todo this folder is public!
                    "Principal": {"AWS": "*"},
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{self.bucket_name}/*",
                }
            ],
        }

        try:
            self.client.make_bucket(self.bucket_name)
            self.client.set_bucket_policy(self.bucket_name, json.dumps(policy_read_only))
        except S3Error as err:
            if err.code != "BucketAlreadyOwnedByYou":
                raise

        return MinIOResource(resource=f"minio://{self.bucket_name}/")

    def put_file(self, file_path: Union[str, Path], rename: Optional[str] = None) -> MinIOResource:
        if isinstance(file_path, Path):
            file_path = str(file_path)

        file_name = Path(file_path).name if not rename else rename

        # copy file to current task directory
        if not file_path.startswith(str(self.local_dir)):
            file_path = shutil.copy(file_path, Path(self.local_dir, file_name))

        object_name = str(Path(self.folder_name, file_name))

        try:
            self.client.fput_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                file_path=file_path,
            )
        except S3Error as err:
            logger.error(f"Could not upload file {object_name} to {self.bucket_name}")
            logger.exception(err)
            raise

        return MinIOResource(resource=f"minio://{self.bucket_name}/{self.folder_name}/{file_name}")

    def get_file(self, data_file: str) -> str:
        if not data_file.startswith("minio://"):
            raise NotValidScheme(f"Object file prefix for '{data_file}' is invalid: expected `minio://`")

        # remove scheme and deconstruct path
        bucket_name, object_name = data_file[len("minio://") :].split("/", 1)

        # todo to this point, `bucket_name` should be task parent id
        # todo  otherwise, this file doesn't belong to this workflow

        file_path = Path(self.temp_dir, bucket_name, object_name)

        # in some cases, the output directory might be missing
        #  e.g., when the `data_file` comes from another workflow
        file_path.parents[0].mkdir(parents=True, exist_ok=True)

        file_lock = FileLock(str(file_path) + ".lock")  # avoid race: https://github.com/minio/minio-py/issues/854

        with file_lock:
            if not file_path.is_file():
                try:
                    self.client.fget_object(
                        bucket_name=bucket_name,
                        object_name=object_name,
                        file_path=(str(file_path)),
                    )
                except S3Error as err:
                    logger.error(f"Could not get file {object_name} from {self.bucket_name}")
                    logger.exception(err)
                    raise

        return str(file_path)

    def remove_remote_dir(self, omit_files: List[str] = None) -> None:
        # todo
        pass
