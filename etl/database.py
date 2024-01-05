import os
import psycopg2
from typing import Optional
from psycopg2 import OperationalError
from psycopg2.pool import SimpleConnectionPool
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connection pool configuration
connection_pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
)


def establish_connection() -> Optional[psycopg2.extensions.connection]:
    try:
        return connection_pool.getconn()
    except OperationalError as e:
        logger.error(f"Error: Unable to connect to the database. {e}")
        return None
