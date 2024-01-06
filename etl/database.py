import os
import psycopg2
from contextlib import contextmanager
from dotenv import load_dotenv
from typing import Optional
from psycopg2 import OperationalError
from psycopg2.pool import SimpleConnectionPool
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IS_TEST_ENV = os.environ.get("TEST_ENVIRONMENT", "False") == "True"

connection_pool = None
# Connection pool configuration

if not IS_TEST_ENV:
    connection_pool = SimpleConnectionPool(
        minconn=1,
        maxconn=10,
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )


@contextmanager
def establish_connection() -> Optional[psycopg2.extensions.connection]:
    if not IS_TEST_ENV and connection_pool:
        try:
            conn = connection_pool.getconn()
            yield conn
        except OperationalError as e:
            logger.error(f"Error: Unable to connect to the database. {e}")
    else:
        yield None
