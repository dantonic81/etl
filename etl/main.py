import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from urllib.parse import urlparse, parse_qs
from typing import Dict


load_dotenv()


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
        'ad_bucket': query_params.get('a_bucket', [''])[0],
        'ad_type': query_params.get('a_type', [''])[0],
        'ad_source': query_params.get('a_source', [''])[0],
        'schema_version': query_params.get('a_v', [''])[0],
        'ad_campaign_id': query_params.get('a_g_campaignid', [''])[0],
        'ad_keyword': query_params.get('a_g_keyword', [''])[0],
        'ad_group_id': query_params.get('a_g_adgroupid', [''])[0],
        'ad_creative': query_params.get('a_g_creative', [''])[0],
    }

    return data


def main():
    """
    Main ETL function to read data from a CSV file, parse URLs, and insert data into PostgreSQL.
    """
    df = pd.read_csv('data/raw_urls.csv')

    # Add columns to DataFrame with parsed data
    df['parsed_data'] = df['url'].apply(parse_url)

    # Connect to PostgreSQL using a context manager
    with psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    ) as connection:
        with connection.cursor() as cursor:
            # Create customer_visits table if not exists
            cursor.execute("""
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
            """)
            connection.commit()

            # Insert data into PostgreSQL
            for index, row in df.iterrows():
                data = row['parsed_data']
                cursor.execute("""
                    INSERT INTO customer_visits 
                    (ad_bucket, ad_type, ad_source, schema_version, ad_campaign_id, ad_keyword, ad_group_id, ad_creative)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    data['ad_bucket'],
                    data['ad_type'],
                    data['ad_source'],
                    data['schema_version'],
                    data['ad_campaign_id'],
                    data['ad_keyword'],
                    data['ad_group_id'],
                    data['ad_creative'],
                ))


if __name__ == "__main__":
    main()