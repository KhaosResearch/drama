import shutil
from pathlib import Path
from socket import gaierror
from typing import List, Optional, Union

from hdfs import InsecureClient
from urllib3.exceptions import NewConnectionError

from drama.config import settings
from drama.storage.base import NotValidScheme, Resource, Storage


class HDFSResource(Resource):
    scheme = "hdfs://"


class HDFSStorage(Storage):
    def __init__(self, bucket_name: str, folder_name: str):
        super().__init__(bucket_name, folder_name)
        self.client = InsecureClient(url=settings.HDFS_CONN, user=settings.HDFS_USERNAME)

    def setup(self) -> HDFSResource:
        super().setup()

        self.client.makedirs(f"{self.bucket_name}/{self.folder_name}")

        return HDFSResource(resource=f"hdfs:/{self.bucket_name}/{self.folder_name}/")

    def put_file(self, file_path: Union[str, Path], rename: Optional[str] = None) -> HDFSResource:
        if isinstance(file_path, Path):
            file_path = str(file_path)

        file_name = Path(file_path).name if not rename else rename

        # copy file to task directory
        if not file_path.startswith(str(self.local_dir)):
            file_path = shutil.copy(file_path, Path(self.local_dir, file_name))

        try:
            self.client.upload(f"{self.bucket_name}/{self.folder_name}/{file_name}", file_path)
        except (gaierror, NewConnectionError):
            raise

        return HDFSResource(resource=f"hdfs:/{self.bucket_name}/{self.folder_name}/{file_name}")

    def get_file(self, data_file: str) -> str:
        if not data_file.startswith("hdfs:"):
            raise NotValidScheme("Object file prefix is invalid: expected `hdfs:`")

        _, bucket_name, folder_name, file_name = data_file.split("/")
        file_path = Path(self.temp_dir, bucket_name, folder_name, file_name)

        if not file_path.is_file():
            try:
                self.client.download(data_file, file_path)
            except Exception as err:
                print(err)

        return str(file_path)

    def remove_remote_dir(self, omit_files: List[str] = None) -> None:
        pass
