import os

os.environ["TEST_ENVIRONMENT"] = "True"

from etl.main import (
    parse_url,
    create_table,
    check_record_exists,
    batch_insert_records,
    main,
)


def test_parse_url():
    url = "https://example.com?a_bucket=value1&a_type=value2&a_source=value3"
    expected_result = {
        "ad_bucket": "value1",
        "ad_type": "value2",
        "ad_source": "value3",
        "schema_version": None,
        "ad_campaign_id": None,
        "ad_keyword": None,
        "ad_group_id": None,
        "ad_creative": None,
    }
    assert parse_url(url) == expected_result


def test_create_table(mocker, mock_psycopg2):
    cursor_mock = mocker.MagicMock()
    create_table(cursor_mock)
    cursor_mock.execute.assert_called_once()


def test_check_record_exists(mocker, mock_psycopg2):
    cursor_mock = mocker.MagicMock()
    data = {
        "ad_bucket": "value1",
        "ad_type": "value2",
        "ad_source": "value3",
        "schema_version": 4,
        "ad_campaign_id": 5,
        "ad_keyword": "value6",
        "ad_group_id": 7,
        "ad_creative": 8,
    }

    # Mock the fetchone result to return count
    cursor_mock.fetchone.return_value = (1,)

    result = check_record_exists(cursor_mock, data)

    cursor_mock.execute.assert_called_once_with(
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

    assert result is True


def test_batch_insert_records(mocker, mock_psycopg2):
    cursor_mock = mocker.MagicMock()
    data_list = [
        {
            "ad_bucket": "value1",
            "ad_type": "value2",
            "ad_source": "value3",
            "schema_version": "value4",
            "ad_campaign_id": "value5",
            "ad_keyword": "value6",
            "ad_group_id": "value7",
            "ad_creative": "value8",
        },
    ]

    batch_insert_records(cursor_mock, data_list)

    expected_executemany_args = (
        """
        INSERT INTO customer_visits 
        (ad_bucket, ad_type, ad_source, schema_version, ad_campaign_id, ad_keyword, ad_group_id, ad_creative)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """,
        [
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
        ],
    )

    cursor_mock.executemany.assert_called_once_with(*expected_executemany_args)


def test_main(mocker, mock_pd, mock_establish_connection):
    # Setup mock DataFrame
    mock_df = mocker.MagicMock()
    mock_df["url"] = [
        "https://example.com?a_bucket=value1&a_type=value2&a_source=value3"
    ]
    mock_df["parsed_data"] = [
        {"ad_bucket": "value1", "ad_type": "value2", "ad_source": "value3"}
    ]
    mock_pd.read_csv.return_value = mock_df

    # Setup mock database connection and cursor
    mock_connection = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_establish_connection.return_value.__enter__.return_value = mock_connection
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

    main()

    # Assertions
    mock_pd.read_csv.assert_called_once_with("data/raw_urls.csv")
    mock_establish_connection.assert_called_once()
    mock_establish_connection.return_value.__enter__.assert_called_once()
    mock_connection.cursor.assert_called_once()
    mock_connection.cursor.return_value.__enter__.assert_called_once()
    mock_cursor.execute.assert_called_once()
