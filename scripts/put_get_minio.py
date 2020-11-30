import json
from pathlib import Path

from minio import Minio, ResponseError
from minio.error import BucketAlreadyOwnedByYou
from pydantic import BaseSettings


class _Settings(BaseSettings):
    MINIO_HOST: str
    MINIO_PORT: int
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str

    class Config:
        assert Path(".env").is_file(), "`.env` not found in current directory"
        env_file = ".env"

    @property
    def MINIO_CONN(self):
        return f"{self.MINIO_HOST}:{self.MINIO_PORT}"


settings = _Settings()

client = Minio(
    settings.MINIO_CONN,
    access_key=settings.MINIO_ACCESS_KEY,
    secret_key=settings.MINIO_SECRET_KEY,
    secure=False,
)

temp_dir = "/Users/benhid/Documents/dramapi/tempdir"

task_id = "taskid"
task_name = "taskname"


def create_bucket(bucket_name: str):
    policy_read_only = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket_name}/*",
            }
        ],
    }

    try:
        client.make_bucket(bucket_name)
        client.set_bucket_policy(bucket_name, json.dumps(policy_read_only))
    except BucketAlreadyOwnedByYou:
        pass


create_bucket(task_id)


def put_file(filepath: str, filename: str = None):
    if not filename:
        filename = Path(filepath).name
    try:
        client.fput_object(
            bucket_name=task_id,
            object_name=str(Path(task_name, filename)),
            file_path=filepath,
        )
    except ResponseError:
        raise

    return f"minio:/{task_id}/{task_name}/{filename}"


# file_from_minio = put_file('/Users/benhid/Documents/dramapi/LICENSE')
# print(file_from_minio)


def get_file(file: str):
    if not file.startswith("minio:"):
        raise Exception("Object file not stored in MinIO")

    _, task_id, task_file = file.split("/", 2)
    filepath = Path(temp_dir, task_id, task_file)

    if not filepath.is_file():
        try:
            client.fget_object(
                bucket_name=task_id,
                object_name=task_file,
                file_path=(str(filepath)),
            )
        except ResponseError as err:
            print(err)

    return str(filepath)


#print(get_file("minio:/test123/testtask1/LICENSE"))
