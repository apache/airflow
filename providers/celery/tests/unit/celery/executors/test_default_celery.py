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

import ssl
from unittest import mock

import pytest

from airflow.providers.celery.executors.default_celery import (
    _broker_supports_visibility_timeout,
    _broker_transport_options,
    get_default_celery_config,
)
from airflow.providers.common.compat.sdk import AirflowException, conf

from tests_common.test_utils.config import conf_vars


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
    def test_default_visibility_timeout_added_for_supporting_broker(self):
        options = _broker_transport_options("redis://localhost:6379/0", conf)

        assert options["visibility_timeout"] == 86400

    def test_no_visibility_timeout_for_non_supporting_broker(self):
        options = _broker_transport_options("amqp://guest@rabbitmq:5672//", conf)

        assert "visibility_timeout" not in options

    @conf_vars({("celery_broker_transport_options", "visibility_timeout"): "21600"})
    def test_configured_visibility_timeout_is_kept(self):
        options = _broker_transport_options("redis://localhost:6379/0", conf)

        assert options["visibility_timeout"] == 21600

    @conf_vars({("celery_broker_transport_options", "sentinel_kwargs"): '{"service_name": "mymaster"}'})
    def test_dict_option_parsed_from_json_string(self):
        options = _broker_transport_options("sentinel://localhost:26379", conf)

        assert options["sentinel_kwargs"] == {"service_name": "mymaster"}

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

    @mock.patch.dict(
        "os.environ", {"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql://airflow@postgres/airflow"}
    )
    @conf_vars({("celery", "result_backend"): None})
    def test_result_backend_falls_back_to_sql_alchemy_conn_with_explicit_driver(self):
        config = get_default_celery_config(conf)

        assert config["result_backend"].startswith("db+postgresql+psycopg")
        assert config["result_backend"].endswith("://airflow@postgres/airflow")

    @mock.patch.dict("os.environ", {"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "mysql://airflow@mysql/airflow"})
    @conf_vars({("celery", "result_backend"): None})
    def test_result_backend_fallback_keeps_non_postgres_scheme(self):
        config = get_default_celery_config(conf)

        assert config["result_backend"] == "db+mysql://airflow@mysql/airflow"

    @conf_vars({("celery", "extra_celery_config"): '{"worker_concurrency": 42, "custom_key": "custom"}'})
    def test_extra_celery_config_overrides_and_extends(self):
        config = get_default_celery_config(conf)

        assert config["worker_concurrency"] == 42
        assert config["custom_key"] == "custom"

    @conf_vars(
        {("celery_result_backend_transport_options", "sentinel_kwargs"): '{"service_name": "mymaster"}'}
    )
    def test_result_backend_sentinel_kwargs_parsed(self):
        config = get_default_celery_config(conf)

        assert config["result_backend_transport_options"]["sentinel_kwargs"] == {"service_name": "mymaster"}

    @conf_vars({("celery_result_backend_transport_options", "sentinel_kwargs"): "{broken"})
    def test_result_backend_sentinel_kwargs_invalid_raises(self):
        with pytest.raises(AirflowException, match="sentinel_kwargs"):
            get_default_celery_config(conf)

    def test_falls_back_to_global_conf_without_getsection(self):
        config = get_default_celery_config(object())

        assert config["broker_url"] == conf.get("celery", "BROKER_URL", fallback="redis://redis:6379/0")


class TestSslConfiguration:
    def test_ssl_inactive_by_default(self):
        config = get_default_celery_config(conf)

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

    @conf_vars(
        {
            ("celery", "ssl_active"): "True",
            ("celery", "broker_url"): "amqps://guest@rabbitmq:5671//",
            ("celery", "ssl_key"): "/keys/client.key",
            ("celery", "ssl_cert"): "/keys/client.crt",
            ("celery", "ssl_cacert"): "/keys/ca.crt",
        }
    )
    def test_amqp_broker_uses_amqp_ssl_keys(self):
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
