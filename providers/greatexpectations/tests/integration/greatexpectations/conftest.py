from __future__ import annotations

import os
import random
import string
from typing import TYPE_CHECKING, Callable, Generator

import great_expectations as gx
import pytest
from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from pathlib import Path

    from _pytest.config import Config
    from _pytest.nodes import Item
    from great_expectations.data_context import AbstractDataContext


def rand_name() -> str:
    return "".join(random.choices(string.ascii_lowercase, k=10))


def is_valid_gx_cloud_url(url: str) -> bool:
    return url.startswith("https://app.greatexpectations.io/organizations/")


@pytest.fixture
def table_name() -> str:
    return "test_table"


@pytest.fixture
def postgres_connection_string() -> str:
    pg_user = os.environ["POSTGRES_USER"]
    pg_pw = os.environ["POSTGRES_PASSWORD"]
    pg_port = os.environ["POSTGRES_PORT"]
    pg_db = os.environ["POSTGRES_DB"]
    return f"postgresql+psycopg2://{pg_user}:{pg_pw}@localhost:{pg_port}/{pg_db}"


@pytest.fixture
def cloud_context() -> AbstractDataContext:
    return gx.get_context(mode="cloud")


@pytest.fixture
def ensure_checkpoint_cleanup(
    ensure_validation_definition_cleanup,
    cloud_context: AbstractDataContext,
) -> Generator[Callable[[str], None], None, None]:
    to_cleanup: set[str] = set()

    def ensure_cleanup(name: str) -> None:
        to_cleanup.add(name)

    yield ensure_cleanup

    for name in to_cleanup:
        cloud_context.checkpoints.delete(name)


@pytest.fixture
def ensure_validation_definition_cleanup(
    ensure_suite_cleanup,
    ensure_data_source_cleanup,
    cloud_context: AbstractDataContext,
) -> Generator[Callable[[str], None], None, None]:
    to_cleanup: set[str] = set()

    def ensure_cleanup(name: str) -> None:
        to_cleanup.add(name)

    yield ensure_cleanup

    for name in to_cleanup:
        cloud_context.validation_definitions.delete(name)


@pytest.fixture
def ensure_suite_cleanup(
    cloud_context: AbstractDataContext,
) -> Generator[Callable[[str], None], None, None]:
    to_cleanup: set[str] = set()

    def ensure_cleanup(name: str) -> None:
        to_cleanup.add(name)

    yield ensure_cleanup

    for name in to_cleanup:
        cloud_context.suites.delete(name)


@pytest.fixture
def ensure_data_source_cleanup(
    cloud_context: AbstractDataContext,
) -> Generator[Callable[[str], None], None, None]:
    to_cleanup: set[str] = set()

    def ensure_cleanup(name: str) -> None:
        to_cleanup.add(name)

    yield ensure_cleanup

    for name in to_cleanup:
        try:
            cloud_context.data_sources.delete(name)
        except KeyError:
            # TODO: remove Try/Except block after CORE-767 is resolved in GX Core
            pass


@pytest.fixture
def load_postgres_data(
    postgres_connection_string: str,
    table_name: str,
) -> Generator[Callable[[list[dict]], None], None, None]:
    """Loads data into a table called `test_table` in the Postgres database.

    This will have a string column called name, and an int column called age.
    This should be enough to cover our use cases.
    """

    def _load_postgres_data(data: list[dict]) -> None:
        engine = create_engine(url=postgres_connection_string)
        with engine.connect() as conn, conn.begin():
            conn.execute(
                text(f"CREATE TABLE {table_name} (name VARCHAR(255), age INT);")
            )
            conn.execute(
                text(f"INSERT INTO {table_name} (name, age) VALUES (:name, :age);"),
                data,
            )

    yield _load_postgres_data

    engine = create_engine(url=postgres_connection_string)
    with engine.connect() as conn, conn.begin():
        conn.execute(text(f"DROP TABLE {table_name};"))


@pytest.fixture
def load_csv_data() -> Generator[Callable[[Path, list[dict]], None], None, None]:
    def _load_csv_data(path: Path, data: list[dict]) -> None:
        with path.open("w") as f:
            f.write("name,age\n")
            for row in data:
                f.write(f"{row['name']},{row['age']}\n")

    yield _load_csv_data
