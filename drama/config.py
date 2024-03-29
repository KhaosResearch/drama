import platform
import tempfile
from pathlib import Path

from pydantic import AnyUrl, BaseModel, BaseSettings, SecretStr

from drama.logger import get_logger

logger = get_logger(__name__)


class MongoDns(AnyUrl):
    allowed_schemes = {"mongodb"}
    user_required = True


class RabbitDns(AnyUrl):
    allowed_schemes = {"amqp"}
    user_required = True


class ActorOpts(BaseModel):
    queue_name: str = "default"
    max_retries: int = 0
    time_limit: int = 3600000 * 7
    notify_shutdown: bool = True


class _Settings(BaseSettings):
    # api settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8080
    API_DEBUG: bool = False
    API_KEY: str = "8ce654d9-0d68-4576-bad1-73794fa163f4"
    API_KEY_NAME: str = "access_token"

    # this private key is used for secrets management
    SECRETS_SK_KEY: SecretStr = "pLxrD3Jy45+Y8Fixo//xj0LMf11p1+y1sfG56LcqyEk="

    # for applications sub-mounted below a given URL path
    ROOT_PATH: str = ""

    # database connection
    MONGO_DNS: MongoDns = "mongodb://root:root@localhost:27017"

    # amqp connection
    RABBIT_DNS: RabbitDns = "amqp://rabbit:rabbit@localhost:5672"

    # to override:
    # >>> export DEFAULT_ACTOR_OPTS='{"max_retries": 1}'
    DEFAULT_ACTOR_OPTS: ActorOpts = ActorOpts()

    # Apache Kafka
    KAFKA_BROKER_HOST: str = "localhost"
    KAFKA_BROKER_PORT: int = 9092

    # DFS
    MINIO_HOST: str = None
    MINIO_USE_SSL: bool = True
    MINIO_BUCKET: str = None
    MINIO_ACCESS_KEY: str = "minio"
    MINIO_SECRET_KEY: str = "minio"

    HDFS_USERNAME: str = "root"
    HDFS_HOST: str = ""
    HDFS_PORT: int = 9000

    DATA_DIR: str = str(Path("/tmp" if platform.system() == "Darwin" else tempfile.gettempdir()))

    @property
    def KAFKA_CONN(self):
        return f"{self.KAFKA_BROKER_HOST}:{self.KAFKA_BROKER_PORT}"

    @property
    def HDFS_CONN(self):
        return f"{self.HDFS_HOST}:{self.HDFS_PORT}"

    class Config:
        env_file = ".env"
        file_path = Path(env_file)
        if not file_path.is_file():
            logger.warning("⚠️ `.env` not found in current directory")
            logger.info("⚙️ Loading settings from environment")
        else:
            logger.info(f"⚙️ Loading settings from dotenv @ {file_path.absolute()}")


settings = _Settings()
