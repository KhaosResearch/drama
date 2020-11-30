from pathlib import Path

from minio import Minio
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


def delete_bucket(bucket_name):
    objects = client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        client.remove_object(bucket_name, obj.object_name)
    client.remove_bucket(bucket_name)


for bucket in client.list_buckets():
    delete_bucket(bucket.name)
