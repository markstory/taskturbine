import os
import psycopg2
import pytest

from taskturbine import Config


def pytest_runtestloop():
    # Clear storage tables at the beginning of each run
    db_url = os.getenv("TASKTURBINE_DATABASE_URL")
    assert db_url, "Required environment variable TASKTURBINE_DATABASE_URL undefined"

    connection = psycopg2.connect(db_url)
    with connection.cursor() as cursor:
        # TODO this isn't working?!
        for table in ("events", "waits", "runs", "tasks"):
            query = f"TRUNCATE taskturbine.{table}"
            cursor.execute(query, [])


@pytest.fixture
def channel(request):
    """Each test should have a unique channel name to reduce bleed through"""
    return request.node.name


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
