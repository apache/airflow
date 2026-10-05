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
import ssl
from unittest import mock

import pytest
from celery import current_app
from celery.contrib.testing.app import setup_default_app
from sqlalchemy import event
from sqlalchemy.engine import make_url

from airflow.providers.celery.executors import default_celery
from airflow.providers.celery.executors.celery_executor_utils import create_celery_app
from airflow.providers.celery.executors.default_celery import (
    _broker_supports_visibility_timeout,
    _broker_transport_options,
    get_default_celery_config,
)
from airflow.providers.common.compat.sdk import AirflowException, conf

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

if AIRFLOW_V_3_2_PLUS:
    from airflow.executors.base_executor import ExecutorConf

requires_psycopg3 = pytest.mark.skipif(
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


@requires_psycopg3
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


@requires_psycopg3
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


@requires_psycopg3
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
@requires_psycopg3
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
@requires_psycopg3
def test_unknown_database_dialect_keeps_engine_options():
    config = default_celery.get_default_celery_config(conf)
    assert config["result_backend"] == "db+bad_schema:///"
    assert config["database_engine_options"] == {"pool_recycle": 1800}


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("redis://localhost:6379/0", True),
        ("rediss://localhost:6379/0", True),
        ("sqs://", True),
        ("sentinel://localhost:26379", True),
        ("amqp://guest@rabbitmq:5672//", False),
        ("amqps://guest@rabbitmq:5671//", False),
        ("db+postgresql://airflow@postgres/airflow", False),
    ],
)
def test_broker_supports_visibility_timeout(url, expected):
    assert _broker_supports_visibility_timeout(url) is expected


class TestBrokerTransportOptions:
    @mock.patch("airflow.providers.celery.executors.default_celery.log.warning", autospec=True)
    def test_default_visibility_timeout_added_with_warning_for_supporting_broker(self, mock_warning):
        options = _broker_transport_options("redis://localhost:6379/0", conf)

        assert options["visibility_timeout"] == 86400
        assert "No visibility_timeout configured" in mock_warning.call_args.args[0]

    def test_no_visibility_timeout_for_non_supporting_broker(self):
        options = _broker_transport_options("amqp://guest@rabbitmq:5672//", conf)

        assert "visibility_timeout" not in options

    @conf_vars({("celery_broker_transport_options", "visibility_timeout"): "21600"})
    @mock.patch("airflow.providers.celery.executors.default_celery.log.warning", autospec=True)
    def test_configured_visibility_timeout_is_kept_without_warning(self, mock_warning):
        options = _broker_transport_options("redis://localhost:6379/0", conf)

        assert options["visibility_timeout"] == 21600
        mock_warning.assert_not_called()

    @pytest.mark.parametrize(
        ("option", "value", "expected"),
        [
            ("client-config", '{"connect_timeout": 5}', {"connect_timeout": 5}),
            (
                "fetch_message_attributes",
                '{"MessageSystemAttributeNames": ["SenderId"], "MessageAttributeNames": ["S3MessageBodyKey"]}',
                {
                    "MessageSystemAttributeNames": ["SenderId"],
                    "MessageAttributeNames": ["S3MessageBodyKey"],
                },
            ),
            ("kafka_consumer_config", '{"group.id": "myconsumer"}', {"group.id": "myconsumer"}),
            (
                "kafka_producer_config",
                '{"ssl.certificate.location": "/foo/bar"}',
                {"ssl.certificate.location": "/foo/bar"},
            ),
            ("sentinel_kwargs", '{"service_name": "mymaster"}', {"service_name": "mymaster"}),
        ],
        ids=lambda case: case if isinstance(case, str) else "",
    )
    def test_dict_option_parsed_from_json_string(self, option, value, expected):
        with conf_vars({("celery_broker_transport_options", option): value}):
            options = _broker_transport_options("sentinel://localhost:26379", conf)

        assert options[option] == expected

    @pytest.mark.parametrize(
        "value",
        [
            "{not valid json",
            '["a", "list"]',
        ],
        ids=["invalid-json", "non-dict-json"],
    )
    def test_dict_option_with_bad_value_raises(self, value):
        with conf_vars({("celery_broker_transport_options", "sentinel_kwargs"): value}):
            with pytest.raises(ValueError, match="sentinel_kwargs.*correct JSON format"):
                _broker_transport_options("sentinel://localhost:26379", conf)


class TestGetDefaultCeleryConfig:
    def test_defaults(self):
        config = get_default_celery_config(conf)

        assert config["accept_content"] == ["json"]
        assert config["event_serializer"] == "json"
        assert config["worker_prefetch_multiplier"] == 1
        assert config["task_acks_late"] is True
        assert config["task_track_started"] is True
        assert config["worker_concurrency"] == 16
        assert config["worker_enable_remote_control"] is True
        assert config["task_default_queue"] == config["task_default_exchange"]

    @conf_vars({("celery", "result_backend"): "db+postgresql://airflow@postgres/airflow"})
    def test_explicit_result_backend_wins(self):
        config = get_default_celery_config(conf)

        assert config["result_backend"] == "db+postgresql://airflow@postgres/airflow"

    @pytest.mark.parametrize(
        ("use_psycopg3", "expected"),
        [
            (True, "db+postgresql+psycopg://user:pass@host/db"),
            (False, "db+postgresql+psycopg2://user:pass@host/db"),
        ],
    )
    @conf_vars(
        {
            ("core", "sql_alchemy_conn"): None,
            ("database", "sql_alchemy_conn"): "postgresql://user:pass@host/db",
            ("celery", "result_backend"): None,
        }
    )
    def test_result_backend_falls_back_to_sql_alchemy_conn_with_explicit_driver(
        self, monkeypatch, use_psycopg3, expected
    ):
        monkeypatch.setattr("airflow.providers.celery.executors.default_celery._USE_PSYCOPG3", use_psycopg3)
        config = get_default_celery_config(conf)

        assert config["result_backend"] == expected

    @conf_vars(
        {
            ("core", "sql_alchemy_conn"): None,
            ("database", "sql_alchemy_conn"): "mysql://airflow@mysql/airflow",
            ("celery", "result_backend"): None,
        }
    )
    def test_result_backend_fallback_keeps_non_postgres_scheme(self):
        config = get_default_celery_config(conf)

        assert config["result_backend"] == "db+mysql://airflow@mysql/airflow"

    @conf_vars({("celery", "extra_celery_config"): '{"worker_concurrency": 42, "custom_key": "custom"}'})
    def test_extra_celery_config_overrides_and_extends(self):
        config = get_default_celery_config(conf)

        assert config["worker_concurrency"] == 42
        assert config["custom_key"] == "custom"

    @conf_vars(
        {
            ("celery", "result_backend"): "sentinel://sentinel1:26379;sentinel://sentinel2:26379",
            ("celery_result_backend_transport_options", "sentinel_kwargs"): '{"password": "redis_pass"}',
            ("celery_result_backend_transport_options", "master_name"): "mymaster",
        }
    )
    def test_result_backend_sentinel_configuration(self):
        config = get_default_celery_config(conf)

        assert config["result_backend"] == "sentinel://sentinel1:26379;sentinel://sentinel2:26379"
        assert config["result_backend_transport_options"] == {
            "sentinel_kwargs": {"password": "redis_pass"},
            "master_name": "mymaster",
        }

    @pytest.mark.parametrize("value", ["invalid_json", '"not_a_dict"'])
    def test_result_backend_sentinel_kwargs_invalid_raises(self, value):
        config = {("celery_result_backend_transport_options", "sentinel_kwargs"): value}

        with conf_vars(config):
            with pytest.raises(
                AirflowException, match="sentinel_kwargs.*should be written in the correct dictionary format"
            ):
                get_default_celery_config(conf)

    def test_falls_back_to_global_conf_without_getsection(self):
        config = get_default_celery_config(object())

        assert config["broker_url"] == conf.get("celery", "BROKER_URL", fallback="redis://redis:6379/0")

    @conf_vars({("celery", "task_acks_late"): "False"})
    def test_task_acks_late_loaded_from_string(self):
        config = get_default_celery_config(conf)

        assert config["task_acks_late"] is False

    @conf_vars(
        {
            ("celery", "result_backend"): "db+sqlite:///:memory:",
            ("celery", "result_backend_sqlalchemy_engine_options"): '{"pool_recycle": 1800}',
        }
    )
    def test_result_backend_sqlalchemy_engine_options(self):
        config = get_default_celery_config(conf)

        assert config["database_engine_options"] == {"pool_recycle": 1800}

    @conf_vars({("celery", "result_backend"): "rediss://test_user:test_password@localhost:6379/0"})
    @mock.patch("airflow.providers.celery.executors.default_celery.log.warning", autospec=True)
    def test_not_recommended_result_backend_warns_without_leaking_credentials(self, mock_warning):
        get_default_celery_config(conf)

        messages = [
            call.args[0] % tuple(call.args[1:]) if len(call.args) > 1 else call.args[0]
            for call in mock_warning.call_args_list
        ]
        assert any("highly recommended to use an alternative result_backend" in m for m in messages)
        assert all("test_password" not in m for m in messages)


class TestSslConfiguration:
    def test_ssl_inactive_by_default(self):
        config = get_default_celery_config(conf)

        assert "broker_use_ssl" not in config

    @conf_vars(
        {
            ("celery", "ssl_active"): "False",
            ("celery", "broker_url"): "amqps://guest@rabbitmq:5671//",
        }
    )
    def test_amqps_broker_gets_no_ssl_when_inactive(self):
        config = get_default_celery_config(conf)

        assert "broker_use_ssl" not in config

    @conf_vars({("celery", "ssl_active"): "yes"})
    def test_non_boolean_ssl_active_degrades_to_no_ssl(self):
        """A malformed [celery] ssl_active must degrade to a no-SSL config instead of raising."""
        config = get_default_celery_config(conf)

        assert "broker_use_ssl" not in config

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="ExecutorConf requires Airflow 3.2+")
    @conf_vars({("celery", "ssl_active"): "yes"})
    def test_non_boolean_ssl_active_degrades_to_no_ssl_for_executor_conf(self):
        config = get_default_celery_config(ExecutorConf(team_name=None))

        assert "broker_use_ssl" not in config

    @conf_vars(
        {
            ("celery", "ssl_active"): "True",
            ("celery", "broker_url"): "rediss://localhost:6379/0",
            ("celery", "ssl_key"): "/keys/client.key",
            ("celery", "ssl_cert"): "/keys/client.crt",
            ("celery", "ssl_cacert"): "/keys/ca.crt",
        }
    )
    def test_redis_broker_uses_redis_ssl_keys(self):
        config = get_default_celery_config(conf)

        assert config["broker_use_ssl"] == {
            "ssl_cert_reqs": ssl.CERT_REQUIRED,
            "ssl_ca_certs": "/keys/ca.crt",
            "ssl_keyfile": "/keys/client.key",
            "ssl_certfile": "/keys/client.crt",
        }

    @pytest.mark.parametrize("broker_url", ["amqps://guest@rabbitmq:5671//", "amqp://guest@rabbitmq:5672//"])
    def test_amqp_broker_uses_amqp_ssl_keys(self, broker_url):
        with conf_vars(
            {
                ("celery", "ssl_active"): "True",
                ("celery", "broker_url"): broker_url,
                ("celery", "ssl_key"): "/keys/client.key",
                ("celery", "ssl_cert"): "/keys/client.crt",
                ("celery", "ssl_cacert"): "/keys/ca.crt",
            }
        ):
            config = get_default_celery_config(conf)

        assert config["broker_use_ssl"] == {
            "cert_reqs": ssl.CERT_REQUIRED,
            "ca_certs": "/keys/ca.crt",
            "keyfile": "/keys/client.key",
            "certfile": "/keys/client.crt",
        }

    @conf_vars(
        {
            ("celery", "ssl_active"): "True",
            ("celery", "broker_url"): "amqps://guest@rabbitmq:5671//",
            ("celery", "ssl_mutual_tls"): "False",
            ("celery", "ssl_key"): "/keys/client.key",
            ("celery", "ssl_cert"): "/keys/client.crt",
            ("celery", "ssl_cacert"): "/keys/ca.crt",
        }
    )
    def test_amqp_one_way_tls_ignores_client_certificates(self):
        config = get_default_celery_config(conf)

        assert config["broker_use_ssl"] == {
            "cert_reqs": ssl.CERT_REQUIRED,
            "ca_certs": "/keys/ca.crt",
        }

    @conf_vars(
        {
            ("celery", "ssl_active"): "True",
            ("celery", "broker_url"): "amqps://guest@rabbitmq:5671//",
            ("celery", "ssl_key"): "/keys/client.key",
            ("celery", "ssl_cert"): "/keys/client.crt",
            ("celery", "ssl_cacert"): "",
        }
    )
    def test_empty_cacert_falls_back_to_system_cas(self):
        config = get_default_celery_config(conf)

        assert "ca_certs" not in config["broker_use_ssl"]
        assert config["broker_use_ssl"]["cert_reqs"] == ssl.CERT_REQUIRED

    @conf_vars(
        {
            ("celery", "ssl_active"): "True",
            ("celery", "ssl_mutual_tls"): "False",
            ("celery", "broker_url"): "rediss://localhost:6379/0",
            ("celery", "ssl_key"): "",
            ("celery", "ssl_cert"): "",
            ("celery", "ssl_cacert"): "/keys/ca.crt",
        }
    )
    def test_one_way_tls_omits_client_certificates(self):
        config = get_default_celery_config(conf)

        assert config["broker_use_ssl"] == {
            "ssl_cert_reqs": ssl.CERT_REQUIRED,
            "ssl_ca_certs": "/keys/ca.crt",
        }

    @conf_vars(
        {
            ("celery", "ssl_active"): "True",
            ("celery", "broker_url"): "rediss://localhost:6379/0",
            ("celery", "ssl_key"): "",
            ("celery", "ssl_cert"): "",
        }
    )
    def test_mutual_tls_without_client_certificates_raises(self):
        with pytest.raises(ValueError, match="SSL_MUTUAL_TLS is True"):
            get_default_celery_config(conf)

    @conf_vars(
        {
            ("celery", "ssl_active"): "True",
            ("celery", "broker_url"): "sqs://",
            ("celery", "ssl_key"): "/keys/client.key",
            ("celery", "ssl_cert"): "/keys/client.crt",
        }
    )
    def test_unsupported_broker_with_ssl_raises(self):
        with pytest.raises(ValueError, match="does not support SSL_ACTIVE"):
            get_default_celery_config(conf)
