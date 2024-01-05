import os
import psycopg2
from dotenv import load_dotenv
from typing import Optional
from psycopg2 import OperationalError
from psycopg2.pool import SimpleConnectionPool
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IS_TEST_ENV = os.environ.get("TEST_ENVIRONMENT", False)


connection_pool = None
# Connection pool configuration

if not IS_TEST_ENV:
    connection_pool = SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        dbname=os.getenv("POSTGRES_DB") if not IS_TEST_ENV else None,
        user=os.getenv("POSTGRES_USER") if not IS_TEST_ENV else None,
        password=os.getenv("POSTGRES_PASSWORD") if not IS_TEST_ENV else None,
        host=os.getenv("DB_HOST") if not IS_TEST_ENV else None,
        port=os.getenv("DB_PORT") if not IS_TEST_ENV else None,
    )


def establish_connection() -> Optional[psycopg2.extensions.connection]:
    if not IS_TEST_ENV and connection_pool:
        try:
            return connection_pool.getconn()
        except OperationalError as e:
            logger.error(f"Error: Unable to connect to the database. {e}")
            return None
    return None
