import pytest


@pytest.fixture
def mock_psycopg2(mocker):
    return mocker.patch('etl.database.psycopg2')


@pytest.fixture
def mock_pd(mocker):
    return mocker.patch('etl.main.pd')


@pytest.fixture
def mock_establish_connection(mocker):
    return mocker.patch('etl.main.establish_connection')
