import os
import shutil
from pathlib import Path
from typing import List, Optional, Union

from drama.storage.base import Resource, Storage


class LocalResource(Resource):
    pass


class LocalStorage(Storage):
    def put_file(self, file_path: Union[str, Path], rename: Optional[str] = None) -> LocalResource:
        if isinstance(file_path, Path):
            file_path = str(file_path)

        file_name = Path(file_path).name if not rename else rename

        if not file_path.startswith(str(self.local_dir)):
            # copy file to local directory
            file_path = shutil.copy(file_path, Path(self.local_dir, file_name))

        # if file is in task directory, rename
        if rename:
            Path(file_path).rename(Path(self.local_dir, file_name))

        return LocalResource(resource=str(file_path))

    def get_file(self, data_file: str) -> str:
        # Use os.path.isfile(data_file) instead of Path(data_file).is_file()
        # if the data_file may contain a URL. On WIndows systems, the latter
        # will raise a OSError: [WinError 123]. As an alternative, catch the
        # OSError and raise a FileNotFoundError.
        if not os.path.isfile(data_file):
            raise FileNotFoundError()
        return data_file

    def remove_remote_dir(self, omit_files: List[str] = None) -> None:
        pass
