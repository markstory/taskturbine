import os
import psycopg2
import pytest

from taskturbine import Config


@pytest.fixture
def config(database_url) -> Config:
    return Config(app_module="", database_url=database_url)


@pytest.fixture
def database_url() -> str:
    value = os.getenv("TASKTURBINE_DATABASE_URL")
    assert value, "Required environment variable TASKTURBINE_DATABASE_URL undefined"
    return value


@pytest.fixture
def db_connection(database_url):
    return psycopg2.connect(database_url)


def row_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

