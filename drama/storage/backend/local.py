import shutil
from pathlib import Path
from typing import Optional, Union

from drama.storage.base import Storage


class LocalStorage(Storage):
    def put_file(self, file_path: Union[str, Path], rename: Optional[str] = None) -> str:
        if isinstance(file_path, Path):
            file_path = str(file_path)

        file_name = Path(file_path).name if not rename else rename

        if not file_path.startswith(str(self.local_dir)):
            # copy file to local directory
            file_path = shutil.copy(file_path, Path(self.local_dir, file_name))

        # if file is in task directory, rename
        if rename:
            Path(file_path).rename(Path(self.local_dir, file_name))

        return file_path

    def get_file(self, data_file: str) -> str:
        if not Path(data_file).is_file():
            raise FileNotFoundError()
        return data_file
