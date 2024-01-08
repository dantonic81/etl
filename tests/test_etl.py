import os
import pandas as pd

os.environ["TEST_ENVIRONMENT"] = "True"

from etl.main import (
    parse_url,
    create_table,
    check_record_exists,
    batch_insert_records,
    extract,
    INSERT_RECORDS_QUERY,
    CHECK_RECORD_EXISTS_QUERY,
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
        CHECK_RECORD_EXISTS_QUERY,
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
        INSERT_RECORDS_QUERY,
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


def test_extract_empty_data_error(mock_open, mocker):
    # Arrange
    mock_read_csv = mocker.patch("pandas.read_csv", side_effect=pd.errors.EmptyDataError)

    # Act
    result = extract("fake_path.csv")

    # Assert
    assert result.empty
    mock_read_csv.assert_called_once_with(mock_open())


def test_extract_parser_error(mock_open, mocker):
    # Arrange
    mock_read_csv = mocker.patch("pandas.read_csv", side_effect=pd.errors.ParserError("Mocked parser error"))

    # Act
    result = extract("fake_path.csv")

    # Assert
    assert result.empty
    mock_read_csv.assert_called_once_with(mock_open())


def test_extract_successful(mock_open, mocker):
    # Arrange
    mock_read_csv = mocker.patch("pandas.read_csv")
    expected_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    mock_read_csv.return_value = expected_df

    # Act
    result = extract("fake_path.csv")

    # Assert
    assert result.equals(expected_df)
    mock_read_csv.assert_called_once_with(mock_open())
