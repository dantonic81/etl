import pandas as pd
from database import establish_connection
from dotenv import load_dotenv
from urllib.parse import urlparse, parse_qs
from typing import Dict, List
from psycopg2 import OperationalError
import logging

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_url(url: str) -> Dict[str, str]:
    """
    Parse a URL and extract relevant data.

    Parameters:
    - url (str): The URL to parse.

    Returns:
    - dict: A dictionary containing parsed data.
    """
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)

    data = {
        "ad_bucket": query_params.get("a_bucket", [""])[0],
        "ad_type": query_params.get("a_type", [""])[0],
        "ad_source": query_params.get("a_source", [""])[0],
        "schema_version": query_params.get("a_v", [""])[0],
        "ad_campaign_id": query_params.get("a_g_campaignid", [""])[0],
        "ad_keyword": query_params.get("a_g_keyword", [""])[0],
        "ad_group_id": query_params.get("a_g_adgroupid", [""])[0],
        "ad_creative": query_params.get("a_g_creative", [""])[0],
    }

    return data


def create_table(cursor):
    """
    Create customer_visits table if not exists.

    Parameters:
    - cursor: psycopg2.extensions.cursor
    """
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS customer_visits (
            ad_bucket VARCHAR(255),
            ad_type VARCHAR(255),
            ad_source VARCHAR(255),
            schema_version VARCHAR(255),
            ad_campaign_id VARCHAR(255),
            ad_keyword VARCHAR(255),
            ad_group_id VARCHAR(255),
            ad_creative VARCHAR(255)
        );
    """
    )


def check_record_exists(cursor, data) -> bool:
    """
    Check if a similar record already exists.

    Parameters:
    - cursor: psycopg2.extensions.cursor
    - data: dict

    Returns:
    - bool: True if the record exists, False otherwise.
    """
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM customer_visits
        WHERE ad_bucket = %s AND ad_type = %s AND ad_source = %s AND
              schema_version = %s AND ad_campaign_id = %s AND ad_keyword = %s AND
              ad_group_id = %s AND ad_creative = %s;
    """,
        (
            data["ad_bucket"],
            data["ad_type"],
            data["ad_source"],
            data["schema_version"],
            data["ad_campaign_id"],
            data["ad_keyword"],
            data["ad_group_id"],
            data["ad_creative"],
        ),
    )

    count = cursor.fetchone()[0]
    return count > 0


def batch_insert_records(cursor, data_list: List[Dict[str, str]]) -> None:
    """
    Batch insert data into the customer_visits table.

    Parameters:
    - cursor: psycopg2.extensions.cursor
    - data_list: List of dictionaries
    """
    data_to_insert = [
        (
            data["ad_bucket"],
            data["ad_type"],
            data["ad_source"],
            data["schema_version"],
            data["ad_campaign_id"],
            data["ad_keyword"],
            data["ad_group_id"],
            data["ad_creative"],
        )
        for data in data_list
    ]

    cursor.executemany(
        """
        INSERT INTO customer_visits 
        (ad_bucket, ad_type, ad_source, schema_version, ad_campaign_id, ad_keyword, ad_group_id, ad_creative)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """,
        data_to_insert,
    )


def main() -> None:
    """
    Main ETL function to read data from a CSV file, parse URLs, and insert data into PostgreSQL.
    """
    try:
        df = pd.read_csv("data/raw_urls.csv")

        # Add columns to DataFrame with parsed data
        df["parsed_data"] = df["url"].apply(parse_url)

        # Attempt to establish a connection
        with establish_connection() as connection:
            if connection:
                try:
                    with connection.cursor() as cursor:
                        # Create customer_visits table if not exists
                        create_table(cursor)

                        # Insert data into PostgreSQL
                        parsed_data_list = df["parsed_data"].tolist()
                        existing_data = []

                        for data in parsed_data_list:
                            # Check if a similar record already exists
                            if not check_record_exists(cursor, data):
                                existing_data.append(data)

                        # Batch insert only the records that don't exist
                        if existing_data:
                            batch_insert_records(cursor, existing_data)

                except OperationalError as e:
                    logger.error(f"Error executing SQL commands: {e}")

    except Exception:
        logger.exception("An unexpected error occurred:", exc_info=True)


if __name__ == "__main__":
    main()
