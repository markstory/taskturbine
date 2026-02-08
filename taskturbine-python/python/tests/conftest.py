import os
from typing import Any

import psycopg2
import pytest

from taskturbine import Config

Connection = psycopg2._psycopg.connection
Cursor = psycopg2._psycopg.cursor


def pytest_sessionstart() -> None:
    # Clear storage tables at the beginning of each session so that we don't
    # have cross test bleed through.
    db_url = os.getenv("TASKTURBINE_DATABASE_URL")
    assert db_url, "Required environment variable TASKTURBINE_DATABASE_URL undefined"

    connection = psycopg2.connect(db_url)
    with connection.cursor() as cursor:
        cursor.execute("BEGIN")
        for table in ("events", "waits", "runs", "tasks"):
            cursor.execute(f"TRUNCATE taskturbine.{table}")
        cursor.execute("COMMIT")


@pytest.fixture
def channel(request: pytest.FixtureRequest) -> str:
    """Each test should have a unique channel name to reduce bleed through"""
    return request.node.name


@pytest.fixture
def config(database_url: str) -> Config:
    return Config(app_module="", database_url=database_url)


@pytest.fixture
def database_url() -> str:
    value = os.getenv("TASKTURBINE_DATABASE_URL")
    assert value, "Required environment variable TASKTURBINE_DATABASE_URL undefined"
    return value


@pytest.fixture
def db_connection(database_url: str) -> Connection:
    return psycopg2.connect(database_url)


def row_factory(cursor: Cursor, row: tuple[Any, ...] | None) -> dict[str, Any]:
    d: dict[str, Any] = {}
    if not row:
        return d
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
