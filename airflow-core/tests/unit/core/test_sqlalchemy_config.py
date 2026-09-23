#
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

from unittest.mock import call, patch

import pytest
from sqlalchemy.pool import NullPool

from airflow import settings
from airflow.exceptions import AirflowConfigException

from tests_common.test_utils.config import conf_vars

SQL_ALCHEMY_CONNECT_ARGS = {"test": 43503, "dict": {"is": 1, "supported": "too"}}

pytestmark = pytest.mark.db_test


class TestSqlAlchemySettings:
    @pytest.fixture(autouse=True, scope="class")
    def reset(self):
        try:
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(
                    settings,
                    "SQL_ALCHEMY_CONN",
                    "mysql+foobar://user:pass@host/dbname?inline=param&another=param",
                )
                yield
        finally:
            settings.configure_orm()

    @patch("airflow.settings.setup_event_handlers")
    @patch("airflow.settings.scoped_session")
    @patch("airflow.settings.sessionmaker")
    @patch("airflow.settings.create_engine")
    def test_configure_orm_with_default_values(
        self, mock_create_engine, mock_sessionmaker, mock_scoped_session, mock_setup_event_handlers
    ):
        settings.configure_orm()
        expected_kwargs = dict(
            connect_args={}
            if not settings.SQL_ALCHEMY_CONN.startswith("sqlite")
            else {"check_same_thread": False},
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_size=5,
            isolation_level="READ COMMITTED",
            future=True,
        )
        mock_create_engine.assert_called_once_with(
            settings.SQL_ALCHEMY_CONN,
            **expected_kwargs,
        )

    @patch("airflow.settings.setup_event_handlers")
    @patch("airflow.settings.scoped_session")
    @patch("airflow.settings.sessionmaker")
    @patch("airflow.settings.create_engine")
    def test_configure_orm_sqlite_file_based_gets_pool_settings(
        self,
        mock_create_engine,
        mock_sessionmaker,
        mock_scoped_session,
        mock_setup_event_handlers,
        monkeypatch,
    ):
        """SQLAlchemy 2.0+ uses QueuePool for file-based SQLite, so pool settings should be applied."""
        monkeypatch.setattr(settings, "SQL_ALCHEMY_CONN", "sqlite:////tmp/airflow.db")
        settings.configure_orm()
        expected_kwargs = dict(
            connect_args={"check_same_thread": False},
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_size=5,
            future=True,
        )
        assert mock_create_engine.mock_calls == [
            call(
                "sqlite:////tmp/airflow.db",
                **expected_kwargs,
            ),
            call().url.password.__bool__(),
            call().url.password.__iter__(),
        ]

    @pytest.mark.parametrize(
        "conn_str",
        [
            "sqlite://",
            "sqlite:///:memory:",
            "sqlite+pysqlite:///:memory:",
            "sqlite:///:memory:?cache=shared",
            "sqlite:///file::memory:?cache=shared",
        ],
    )
    def test_prepare_engine_args_sqlite_in_memory_skips_pool_settings(self, conn_str, monkeypatch):
        """In-memory SQLite uses SingletonThreadPool which doesn't support pool_size/max_overflow."""
        monkeypatch.setattr(settings, "SQL_ALCHEMY_CONN", conn_str)
        engine_args = settings.prepare_engine_args()
        assert "pool_size" not in engine_args
        assert "max_overflow" not in engine_args
        assert "pool_recycle" not in engine_args
        assert "pool_pre_ping" not in engine_args

    @pytest.mark.parametrize(
        ("conn_str", "use_psycopg3", "expect_psycopg2_tuning"),
        [
            ("postgresql+psycopg2://user:pass@host/db", True, True),
            ("postgresql+psycopg2://user:pass@host/db", False, True),
            ("postgresql+psycopg://user:pass@host/db", True, False),
            ("postgresql+psycopg://user:pass@host/db", False, False),
        ],
    )
    def test_prepare_engine_args_psycopg2_tuning_follows_configured_scheme(
        self, conn_str, use_psycopg3, expect_psycopg2_tuning, monkeypatch
    ):
        """executemany tuning must follow the configured scheme, not whether psycopg happens to be importable."""
        monkeypatch.setattr(settings, "SQL_ALCHEMY_CONN", conn_str)
        monkeypatch.setattr(settings, "_USE_PSYCOPG3", use_psycopg3)
        engine_args = settings.prepare_engine_args()
        assert engine_args["insertmanyvalues_page_size"] == 10000
        if expect_psycopg2_tuning:
            assert engine_args["executemany_mode"] == "values_plus_batch"
            assert engine_args["executemany_batch_page_size"] == 2000
        else:
            assert "executemany_mode" not in engine_args
            assert "executemany_batch_page_size" not in engine_args

    @patch("airflow.settings.setup_event_handlers")
    @patch("airflow.settings.scoped_session")
    @patch("airflow.settings.sessionmaker")
    @patch("airflow.settings.create_engine")
    def test_sql_alchemy_connect_args(
        self, mock_create_engine, mock_sessionmaker, mock_scoped_session, mock_setup_event_handlers
    ):
        config = {
            (
                "database",
                "sql_alchemy_connect_args",
            ): "unit.core.test_sqlalchemy_config.SQL_ALCHEMY_CONNECT_ARGS",
            ("database", "sql_alchemy_engine_args"): '{"arg": 1}',
            ("database", "sql_alchemy_pool_enabled"): "False",
        }
        with conf_vars(config):
            settings.configure_orm()
            engine_args = {"arg": 1}
            if settings.SQL_ALCHEMY_CONN.startswith("mysql"):
                engine_args["isolation_level"] = "READ COMMITTED"
            expected_kwargs = dict(
                connect_args=SQL_ALCHEMY_CONNECT_ARGS,
                poolclass=NullPool,
                future=True,
                **engine_args,
            )
            mock_create_engine.assert_called_once_with(
                settings.SQL_ALCHEMY_CONN,
                **expected_kwargs,
            )

    @patch("airflow.settings.setup_event_handlers")
    @patch("airflow.settings.scoped_session")
    @patch("airflow.settings.sessionmaker")
    @patch("airflow.settings.create_engine")
    def test_sql_alchemy_invalid_connect_args(
        self, mock_create_engine, mock_sessionmaker, mock_scoped_session, mock_setup_event_handlers
    ):
        config = {
            ("database", "sql_alchemy_connect_args"): "does.not.exist",
            ("database", "sql_alchemy_pool_enabled"): "False",
        }
        with pytest.raises(AirflowConfigException):
            with conf_vars(config):
                settings.configure_orm()
