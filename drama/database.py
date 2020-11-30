from dramatiq import get_logger
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ServerSelectionTimeoutError

from drama.config import settings

logger = get_logger(__name__)


class DataBase:
    client: MongoClient = None


db = DataBase()


def create_db_connection() -> Database:
    logger.debug("Connecting to database for the first time")
    client = MongoClient(settings.MONGO_DNS)

    try:
        client.server_info()
    except ServerSelectionTimeoutError:
        raise ConnectionError(f"Could not connect to database with connection string {settings.MONGO_DNS}")

    db.client = client

    return db.client.drama


def close_db_connection() -> None:
    logger.debug("Clossing connection with database")
    db.client.close()


def get_db_connection() -> Database:
    if not db.client:
        logger.debug("Client not connected")
        create_db_connection()
    return db.client.drama
