# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import json

import pytest
from celery import current_app
from celery.contrib.testing.app import setup_default_app
from sqlalchemy import event
from sqlalchemy.engine import make_url

from airflow.providers.celery.executors import default_celery
from airflow.providers.celery.executors.celery_executor_utils import create_celery_app
from airflow.providers.common.compat.sdk import conf

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.skipif(
    not default_celery._USE_PSYCOPG3,
    reason="Psycopg3 result backends require psycopg and SQLAlchemy 2.0+",
)


class ConnectionAttemptIntercepted(Exception):
    pass


@pytest.fixture
def restore_current_celery_app():
    with setup_default_app(current_app._get_current_object()):
        yield


def get_backend_connect_kwargs(backend):
    engine = backend.session_manager.get_engine(backend.url, **backend.engine_options)
    connect_kwargs = {}

    @event.listens_for(engine, "do_connect")
    def capture_connect_kwargs(dialect, connection_record, args, kwargs):
        connect_kwargs.update(kwargs)
        raise ConnectionAttemptIntercepted

    try:
        with pytest.raises(ConnectionAttemptIntercepted):
            engine.connect()
        return engine.dialect.driver, connect_kwargs
    finally:
        engine.dispose()


@pytest.mark.usefixtures("restore_current_celery_app")
@pytest.mark.parametrize(
    "result_backend",
    [
        pytest.param(None, id="derived-from-metadata"),
        pytest.param("db+postgresql+psycopg://user:pass@host/db", id="explicit-psycopg"),
        pytest.param("db+postgresql://user:pass@host/db", id="sqlalchemy-default-driver"),
        pytest.param("db+postgresql+psycopg2://user:pass@host/db", id="explicit-psycopg2"),
    ],
)
@pytest.mark.parametrize(
    "engine_options",
    [
        pytest.param({}, id="default-options"),
        pytest.param({"pool_recycle": 1800, "connect_args": {"connect_timeout": 7}}, id="custom-options"),
    ],
)
def test_result_backend_disables_psycopg_preparation(result_backend, engine_options):
    expected_url = result_backend or "db+postgresql+psycopg://user:pass@host/db"
    expected_driver = make_url(expected_url.removeprefix("db+")).get_driver_name()
    expected_connect_args = {
        **({"prepare_threshold": None} if expected_driver == "psycopg" else {}),
        **engine_options.get("connect_args", {}),
    }
    expected_options = {**engine_options}
    if expected_connect_args:
        expected_options["connect_args"] = expected_connect_args

    with conf_vars(
        {
            # --skip-db-tests sets the deprecated [core] key, which takes precedence over [database].
            ("core", "sql_alchemy_conn"): None,
            ("database", "sql_alchemy_conn"): "postgresql://user:pass@host/db",
            ("celery", "result_backend"): result_backend,
            ("celery", "result_backend_sqlalchemy_engine_options"): json.dumps(engine_options),
            ("celery", "extra_celery_config"): '{"database_create_tables_at_setup": false}',
        }
    ):
        with create_celery_app(conf) as app:
            backend = app.backend
            driver, connect_kwargs = get_backend_connect_kwargs(backend)
            assert driver == expected_driver
            assert backend.engine_options == expected_options
            assert {key: connect_kwargs[key] for key in expected_connect_args} == expected_connect_args
            if expected_driver != "psycopg":
                assert "prepare_threshold" not in connect_kwargs


@pytest.mark.usefixtures("restore_current_celery_app")
@pytest.mark.parametrize("prepare_threshold", [None, 0, 5])
def test_result_backend_preserves_explicit_prepare_threshold(prepare_threshold):
    options = {
        "pool_recycle": 1800,
        "connect_args": {"prepare_threshold": prepare_threshold, "connect_timeout": 7},
    }
    with conf_vars(
        {
            ("celery", "result_backend"): "db+postgresql+psycopg://user:pass@host/db",
            ("celery", "result_backend_sqlalchemy_engine_options"): json.dumps(options),
            ("celery", "extra_celery_config"): '{"database_create_tables_at_setup": false}',
        }
    ):
        with create_celery_app(conf) as app:
            backend = app.backend
            driver, connect_kwargs = get_backend_connect_kwargs(backend)
            assert driver == "psycopg"
            assert backend.engine_options == options
            assert connect_kwargs["prepare_threshold"] == prepare_threshold
            assert connect_kwargs["connect_timeout"] == 7


@pytest.mark.usefixtures("restore_current_celery_app")
@conf_vars(
    {
        ("celery", "result_backend"): "db+sqlite:///:memory:",
        ("celery", "result_backend_sqlalchemy_engine_options"): "{}",
        ("celery", "extra_celery_config"): '{"database_create_tables_at_setup": false}',
    }
)
def test_result_backend_does_not_add_psycopg_options_to_sqlite():
    with create_celery_app(conf) as app:
        backend = app.backend
        driver, connect_kwargs = get_backend_connect_kwargs(backend)
        assert driver == "pysqlite"
        assert backend.engine_options == {}
        assert "prepare_threshold" not in connect_kwargs


@conf_vars(
    {
        ("celery", "result_backend"): "redis://redis:6379/0",
        ("celery", "result_backend_sqlalchemy_engine_options"): '{"pool_recycle": 1800}',
    }
)
def test_non_database_result_backend_keeps_engine_options():
    config = default_celery.get_default_celery_config(conf)
    assert config["result_backend"] == "redis://redis:6379/0"
    assert config["database_engine_options"] == {"pool_recycle": 1800}


@conf_vars(
    {
        ("celery", "result_backend"): "db+bad_schema:///",
        ("celery", "result_backend_sqlalchemy_engine_options"): '{"pool_recycle": 1800}',
    }
)
def test_unknown_database_dialect_keeps_engine_options():
    config = default_celery.get_default_celery_config(conf)
    assert config["result_backend"] == "db+bad_schema:///"
    assert config["database_engine_options"] == {"pool_recycle": 1800}
