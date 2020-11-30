from typing import Type

from drama.config import settings
from drama.logger import get_logger
from drama.storage.backend.hdfs import HDFSStorage
from drama.storage.backend.local import LocalStorage
from drama.storage.backend.minio import MinIOStorage
from drama.storage.base import Storage

logger = get_logger(__name__)


def get_available_storage() -> Type[Storage]:
    """
    Get available storage based on settings. MinIO is preferred over HDFS, and HDFS is preferred over local storage.
    """
    if settings.MINIO_HOST:
        return MinIOStorage
    elif settings.HDFS_HOST:
        logger.debug("MinIO storage not set, falling-back to HDFS storage")
        return HDFSStorage

    logger.debug("Remote storage not set, falling-back to LocalStorage")
    logger.warning("LocalStorage does not support distributed execution")

    return LocalStorage
