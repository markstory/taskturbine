import os
from typing import Any

import logging
import psycopg2
import psycopg2.errors
import pytest

from taskturbine import Config, TaskturbineApp

Connection = psycopg2._psycopg.connection
Cursor = psycopg2._psycopg.cursor


def pytest_sessionstart() -> None:
    logging.basicConfig()

    # Clear storage tables at the beginning of each session so that we don't
    # have cross test bleed through.
    db_url = os.getenv("TASKTURBINE_DATABASE_URL")
    assert db_url, "Required environment variable TASKTURBINE_DATABASE_URL undefined"
    print(f"Using DB_URL {db_url}")

    connection = psycopg2.connect(db_url)
    try:
        with connection.cursor() as cursor:
            cursor.execute("BEGIN")
            for table in ("events", "waits", "runs", "tasks"):
                cursor.execute(f"TRUNCATE taskturbine.{table}")
            cursor.execute("COMMIT")
    except psycopg2.errors.Error as e:
        print(f"DB cleanup failed with {e}")
        print("Creating database schema")
        config = Config(database_url=db_url, app_module="")
        app = TaskturbineApp(config)
        app.update_schema()


@pytest.fixture
def channel(request: pytest.FixtureRequest) -> str:
    """Each test should have a unique channel name to reduce bleed through"""
    return str(request.node.name)


@pytest.fixture
def config(database_url: str) -> Config:
    return Config(app_module="tests.demo:app", database_url=database_url)


@pytest.fixture
def database_url() -> str:
    value = os.getenv("TASKTURBINE_DATABASE_URL")
    assert value, "Required environment variable TASKTURBINE_DATABASE_URL undefined"
    return value


@pytest.fixture
def db_connection(database_url: str) -> Connection:
    return psycopg2.connect(database_url)

def row_factory(columns: list[tuple[str]], row: tuple[Any, ...] | None) -> dict[str, Any]:
    d: dict[str, Any] = {}
    if not row or not columns:
        return d
    for idx, col in enumerate(columns):
        d[col[0]] = row[idx]
    return d

def fetch_one(cursor: Cursor) -> dict[str, Any]:
    return row_factory(cursor.description, cursor.fetchone())

def fetch_all(cursor: Cursor) -> list[dict[str, Any]]:
    """fetch all the rows from a cursor as dicts"""
    return [
        row_factory(cursor.description, row)
        for row in cursor.fetchall()
    ]
