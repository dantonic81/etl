import pandas as pd
from etl.database import establish_connection
from dotenv import load_dotenv
from urllib.parse import urlparse, parse_qs
from typing import Dict, List
from psycopg2 import OperationalError, Error
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
        "ad_bucket": query_params.get("a_bucket", [None])[0],
        "ad_type": query_params.get("a_type", [None])[0],
        "ad_source": query_params.get("a_source", [None])[0],
        "schema_version": int(query_params.get("a_v", [None])[0])
        if query_params.get("a_v")
        else None,
        "ad_campaign_id": int(query_params.get("a_g_campaignid", [None])[0])
        if query_params.get("a_g_campaignid")
        else None,
        "ad_keyword": query_params.get("a_g_keyword", [None])[0],
        "ad_group_id": int(query_params.get("a_g_adgroupid", [None])[0])
        if query_params.get("a_g_adgroupid")
        else None,
        "ad_creative": int(query_params.get("a_g_creative", [None])[0])
        if query_params.get("a_g_creative")
        else None,
    }

    return data


def create_table(cursor):
    """
    Create customer_visits table if not exists.

    Parameters:
    - cursor: psycopg2.extensions.cursor
    """
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS customer_visits (
                ad_bucket VARCHAR(255),
                ad_type VARCHAR(255),
                ad_source VARCHAR(255),
                schema_version INT,
                ad_campaign_id INT,
                ad_keyword VARCHAR(255),
                ad_group_id INT,
                ad_creative INT
            );
        """
        )

    except Error as e:
        logger.error(f"Error creating 'customer_visits' table: {e}")


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
        WHERE 
            (ad_bucket = %s OR ad_bucket IS NULL) AND
            (ad_type = %s OR ad_type IS NULL) AND
            (ad_source = %s OR ad_source IS NULL) AND
            (schema_version = %s OR schema_version IS NULL) AND
            (ad_campaign_id = %s OR ad_campaign_id IS NULL) AND
            (ad_keyword = %s OR ad_keyword IS NULL) AND
            (ad_group_id = %s OR ad_group_id IS NULL) AND
            (ad_creative = %s OR ad_creative IS NULL);
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
    # Count the number of rows before the insert operation
    cursor.execute("SELECT COUNT(*) FROM customer_visits;")
    before_insert_count = cursor.fetchone()[0]

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

    # Log the specific records being inserted
    for data in data_to_insert:
        logger.debug(f"Inserted record: {data}")

    # Count the number of rows after the insert operation
    cursor.execute("SELECT COUNT(*) FROM customer_visits;")
    after_insert_count = cursor.fetchone()[0]

    # Calculate the number of rows inserted
    rows_inserted = after_insert_count - before_insert_count

    # Log the number of rows inserted
    logger.info(f"{rows_inserted} rows inserted.")


def main() -> None:
    """
    Main ETL function to read data from a CSV file, parse URLs, and insert data into PostgreSQL.
    """
    try:
        logger.info("Reading source data...")
        df = pd.read_csv("data/raw_urls.csv")

        # Add columns to DataFrame with parsed data
        df["parsed_data"] = df["url"].apply(parse_url)
        # Log the number of records in the DataFrame
        logger.info(f"Number of records read: {len(df)}")

        # Attempt to establish a connection
        with establish_connection() as connection:
            if connection:
                logger.info("Connected to database.")
                try:
                    with connection.cursor() as cursor:
                        # Create customer_visits table if not exists
                        create_table(cursor)
                        logger.info("Table 'customer_visits' created successfully.")

                        # Insert data into PostgreSQL
                        parsed_data_list = df["parsed_data"].tolist()
                        existing_data = []

                        logger.info("Checking if records already exist...")
                        for data in parsed_data_list:
                            # Check if a similar record already exists
                            if not check_record_exists(cursor, data):
                                existing_data.append(data)

                        # Batch insert only the records that don't exist

                        if existing_data:
                            logger.info("Inserting new records...")
                            batch_insert_records(cursor, existing_data)
                        else:
                            logger.info("No new records to insert.")

                    # Commit changes
                    connection.commit()
                    logger.info("Changes committed.")

                except OperationalError as e:
                    logger.error(f"Error executing SQL commands: {e}")

    except Exception:
        logger.exception("An unexpected error occurred:", exc_info=True)


if __name__ == "__main__":
    main()
