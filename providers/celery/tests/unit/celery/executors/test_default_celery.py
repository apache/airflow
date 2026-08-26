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

import logging

import pytest

from airflow.providers.celery.executors import default_celery
from airflow.providers.common.compat.sdk import conf

from tests_common.test_utils.config import conf_vars


@conf_vars({("celery", "result_backend"): "rediss://test_user:test_password@localhost:6379/0"})
def test_celery_executor_with_no_recommended_result_backend(caplog):
    import importlib

    from airflow.providers.celery.executors.default_celery import log

    with caplog.at_level(logging.WARNING, logger=log.name):
        # Reload celery conf to apply the new config.
        importlib.reload(default_celery)
        assert "test_password" not in caplog.text
        assert (
            "You have configured a result_backend using the protocol `rediss`,"
            " it is highly recommended to use an alternative result_backend (i.e. a database)."
        ) in caplog.text


_dict_options_test_cases: list[tuple[str, str, dict]] = [
    (
        "client-config",
        '{"connect_timeout": 5}',
        {
            "connect_timeout": 5,
        },
    ),
    (
        "fetch_message_attributes",
        """
            {
                "MessageSystemAttributeNames": ["SenderId", "SentTimestamp"],
                "MessageAttributeNames": ["S3MessageBodyKey"]
            }
        """,
        {
            "MessageSystemAttributeNames": ["SenderId", "SentTimestamp"],
            "MessageAttributeNames": ["S3MessageBodyKey"],
        },
    ),
    (
        "predefined_exchanges",
        """
            {
                "exchange-1": {
                    "arn": "arn:aws:sns:us-east-1:xxx:exchange-1",
                    "access_key_id": "a",
                    "secret_access_key": "b"
                },
                "exchange-2.fifo": {
                    "arn": "arn:aws:sns:us-east-1:xxx:exchange-2",
                    "access_key_id": "c",
                    "secret_access_key": "d"
                }
            }
        """,
        {
            "exchange-1": {
                "arn": "arn:aws:sns:us-east-1:xxx:exchange-1",
                "access_key_id": "a",
                "secret_access_key": "b",
            },
            "exchange-2.fifo": {
                "arn": "arn:aws:sns:us-east-1:xxx:exchange-2",
                "access_key_id": "c",
                "secret_access_key": "d",
            },
        },
    ),
    (
        "predefined_queues",
        """
        {
            "queue-1": {
                "url": "https://sqs.us-east-1.amazonaws.com/xxx/aaa",
                "access_key_id": "a",
                "secret_access_key": "b",
                "backoff_tasks": ["svc.tasks.tasks.task1"]
            },
            "queue-2.fifo": {
                "url": "https://sqs.us-east-1.amazonaws.com/xxx/bbb.fifo",
                "access_key_id": "c",
                "secret_access_key": "d"
            }
        }
        """,
        {
            "queue-1": {
                "url": "https://sqs.us-east-1.amazonaws.com/xxx/aaa",
                "access_key_id": "a",
                "secret_access_key": "b",
                "backoff_tasks": ["svc.tasks.tasks.task1"],
            },
            "queue-2.fifo": {
                "url": "https://sqs.us-east-1.amazonaws.com/xxx/bbb.fifo",
                "access_key_id": "c",
                "secret_access_key": "d",
            },
        },
    ),
    (
        "queue_tags",
        """
        {
                "Environment": "production",
                "Team": "backend"
        }
        """,
        {
            "Environment": "production",
            "Team": "backend",
        },
    ),
    (
        "sqs-creation-attributes",
        """
        {
            "KmsMasterKeyId": "alias/aws/sqs"
        }
        """,
        {
            "KmsMasterKeyId": "alias/aws/sqs",
        },
    ),
    (
        "kafka_admin_config",
        '{"sasl.username": "foo", "sasl.password": "bar"}',
        {"sasl.username": "foo", "sasl.password": "bar"},
    ),
    ("kafka_common_config", '{"compression.type": "zstd"}', {"compression.type": "zstd"}),
    (
        "kafka_consumer_config",
        '{"group.id": "myconsumer"}',
        {"group.id": "myconsumer"},
    ),
    (
        "kafka_producer_config",
        '{"ssl.certificate.location": "/foo/bar"}',
        {"ssl.certificate.location": "/foo/bar"},
    ),
    ("sentinel_kwargs", '{"service_name": "mymaster"}', {"service_name": "mymaster"}),
]


@pytest.mark.parametrize(
    (
        "option",
        "value",
        "expected",
    ),
    _dict_options_test_cases,
    ids=[t[0] for t in _dict_options_test_cases],
)
def test_dict_options_loaded_from_string(option, value, expected):
    import importlib

    # Reload celery conf to apply the new config.
    with conf_vars({("celery_broker_transport_options", option): value}):
        importlib.reload(default_celery)
        assert default_celery.DEFAULT_CELERY_CONFIG["broker_transport_options"][option] == expected


@conf_vars({("celery", "task_acks_late"): "False"})
def test_celery_task_acks_late_loaded_from_string():
    import importlib

    # Reload celery conf to apply the new config.
    importlib.reload(default_celery)
    assert default_celery.DEFAULT_CELERY_CONFIG["task_acks_late"] is False


@conf_vars({("celery", "BROKER_URL"): "redis://localhost:6379/0"})
def test_visibility_timeout_default_warns_when_not_configured(caplog):
    """Test that a warning is logged when visibility_timeout defaults to 86400 (24h)."""
    import importlib

    from airflow.providers.celery.executors.default_celery import log

    with caplog.at_level(logging.WARNING, logger=log.name):
        importlib.reload(default_celery)
        assert default_celery.DEFAULT_CELERY_CONFIG["broker_transport_options"]["visibility_timeout"] == 86400
        assert "No visibility_timeout configured" in caplog.text
        assert "86400" in caplog.text
        assert "long-running tasks" in caplog.text


@conf_vars(
    {
        ("celery", "BROKER_URL"): "redis://localhost:6379/0",
        ("celery_broker_transport_options", "visibility_timeout"): "172800",
    }
)
def test_visibility_timeout_no_warning_when_configured(caplog):
    """Test that no warning is logged when visibility_timeout is explicitly configured."""
    import importlib

    from airflow.providers.celery.executors.default_celery import log

    with caplog.at_level(logging.WARNING, logger=log.name):
        importlib.reload(default_celery)
        assert (
            int(default_celery.DEFAULT_CELERY_CONFIG["broker_transport_options"]["visibility_timeout"])
            == 172800
        )
        assert "No visibility_timeout configured" not in caplog.text


@conf_vars({("celery", "BROKER_URL"): "amqp://guest:guest@localhost:5672//"})
def test_visibility_timeout_not_set_for_unsupported_broker(caplog):
    """Test that visibility_timeout is not set for brokers that don't support it (e.g. RabbitMQ)."""
    import importlib

    from airflow.providers.celery.executors.default_celery import log

    with caplog.at_level(logging.WARNING, logger=log.name):
        importlib.reload(default_celery)
        assert "visibility_timeout" not in default_celery.DEFAULT_CELERY_CONFIG.get(
            "broker_transport_options", {}
        )
        assert "No visibility_timeout configured" not in caplog.text


@conf_vars({("celery", "extra_celery_config"): '{"worker_max_tasks_per_child": 10}'})
def test_celery_extra_celery_config_loaded_from_string():
    import importlib

    # Reload celery conf to apply the new config.
    importlib.reload(default_celery)
    assert default_celery.DEFAULT_CELERY_CONFIG["worker_max_tasks_per_child"] == 10


@conf_vars({("celery_result_backend_transport_options", "sentinel_kwargs"): '{"password": "redis_password"}'})
def test_result_backend_sentinel_kwargs_loaded_from_string():
    """Test that sentinel_kwargs for result backend transport options is correctly parsed."""
    import importlib

    # Reload celery conf to apply the new config.
    importlib.reload(default_celery)
    assert "result_backend_transport_options" in default_celery.DEFAULT_CELERY_CONFIG
    assert default_celery.DEFAULT_CELERY_CONFIG["result_backend_transport_options"]["sentinel_kwargs"] == {
        "password": "redis_password"
    }


@conf_vars({("celery_result_backend_transport_options", "master_name"): "mymaster"})
def test_result_backend_master_name_loaded():
    """Test that master_name for result backend transport options is correctly loaded."""
    import importlib

    # Reload celery conf to apply the new config.
    importlib.reload(default_celery)
    assert "result_backend_transport_options" in default_celery.DEFAULT_CELERY_CONFIG
    assert (
        default_celery.DEFAULT_CELERY_CONFIG["result_backend_transport_options"]["master_name"] == "mymaster"
    )


@conf_vars(
    {
        ("celery_result_backend_transport_options", "sentinel_kwargs"): '{"password": "redis_password"}',
        ("celery_result_backend_transport_options", "master_name"): "mymaster",
    }
)
def test_result_backend_transport_options_with_multiple_options():
    """Test that multiple result backend transport options are correctly loaded."""
    import importlib

    # Reload celery conf to apply the new config.
    importlib.reload(default_celery)
    result_backend_opts = default_celery.DEFAULT_CELERY_CONFIG["result_backend_transport_options"]
    assert result_backend_opts["sentinel_kwargs"] == {"password": "redis_password"}
    assert result_backend_opts["master_name"] == "mymaster"


@conf_vars(
    {
        ("celery", "result_backend"): None,
        ("database", "sql_alchemy_conn"): "postgresql://user:pass@host/db",
    }
)
def test_result_backend_derived_from_sql_alchemy_conn_uses_psycopg(monkeypatch):
    """A driverless sql_alchemy_conn must derive a psycopg (v3) result_backend, not psycopg2."""
    monkeypatch.setattr(default_celery, "_USE_PSYCOPG3", True)
    config = default_celery.get_default_celery_config(conf)
    assert config["result_backend"] == "db+postgresql+psycopg://user:pass@host/db"


@conf_vars(
    {
        ("celery", "result_backend"): None,
        ("database", "sql_alchemy_conn"): "postgresql://user:pass@host/db",
    }
)
def test_result_backend_falls_back_to_psycopg2_without_psycopg3(monkeypatch):
    """Without psycopg/SQLAlchemy 2.0 available, the derivation must fall back to psycopg2."""
    monkeypatch.setattr(default_celery, "_USE_PSYCOPG3", False)
    config = default_celery.get_default_celery_config(conf)
    assert config["result_backend"] == "db+postgresql+psycopg2://user:pass@host/db"


@conf_vars({("celery_result_backend_transport_options", "sentinel_kwargs"): "invalid_json"})
def test_result_backend_sentinel_kwargs_invalid_json():
    """Test that invalid JSON in sentinel_kwargs raises an error."""
    import importlib

    from airflow.providers.common.compat.sdk import AirflowException

    with pytest.raises(
        AirflowException, match="sentinel_kwargs.*should be written in the correct dictionary format"
    ):
        importlib.reload(default_celery)


@conf_vars({("celery_result_backend_transport_options", "sentinel_kwargs"): '"not_a_dict"'})
def test_result_backend_sentinel_kwargs_not_dict():
    """Test that non-dict sentinel_kwargs raises an error."""
    import importlib

    from airflow.providers.common.compat.sdk import AirflowException

    with pytest.raises(
        AirflowException, match="sentinel_kwargs.*should be written in the correct dictionary format"
    ):
        importlib.reload(default_celery)


@conf_vars(
    {
        ("celery", "result_backend"): "sentinel://sentinel1:26379;sentinel://sentinel2:26379",
        ("celery_result_backend_transport_options", "sentinel_kwargs"): '{"password": "redis_pass"}',
        ("celery_result_backend_transport_options", "master_name"): "mymaster",
    }
)
def test_result_backend_sentinel_full_config():
    """Test full Redis Sentinel configuration for result backend."""
    import importlib

    # Reload celery conf to apply the new config.
    importlib.reload(default_celery)

    assert default_celery.DEFAULT_CELERY_CONFIG["result_backend"] == (
        "sentinel://sentinel1:26379;sentinel://sentinel2:26379"
    )
    result_backend_opts = default_celery.DEFAULT_CELERY_CONFIG["result_backend_transport_options"]
    assert result_backend_opts["sentinel_kwargs"] == {"password": "redis_pass"}
    assert result_backend_opts["master_name"] == "mymaster"


class TestAmqpsSslConfig:
    """Tests for amqps:// broker URL SSL configuration (Fix for substring match bug)."""

    @conf_vars(
        {
            ("celery", "BROKER_URL"): "amqps://guest:guest@rabbitmq:5671//",
            ("celery", "SSL_ACTIVE"): "True",
            ("celery", "SSL_KEY"): "/path/to/key.pem",
            ("celery", "SSL_CERT"): "/path/to/cert.pem",
            ("celery", "SSL_CACERT"): "/path/to/ca.pem",
        }
    )
    def test_amqps_broker_url_builds_ssl_config(self):
        """Test that amqps:// broker URLs correctly build broker_use_ssl with AMQP param names."""
        import importlib
        import ssl

        importlib.reload(default_celery)

        config = default_celery.DEFAULT_CELERY_CONFIG
        assert "broker_use_ssl" in config, "broker_use_ssl should be set for amqps:// URLs"
        broker_ssl = config["broker_use_ssl"]
        assert broker_ssl["keyfile"] == "/path/to/key.pem"
        assert broker_ssl["certfile"] == "/path/to/cert.pem"
        assert broker_ssl["ca_certs"] == "/path/to/ca.pem"
        assert broker_ssl["cert_reqs"] == ssl.CERT_REQUIRED
        # Must NOT have ssl_ prefixed keys (those are for Redis)
        assert "ssl_keyfile" not in broker_ssl
        assert "ssl_certfile" not in broker_ssl

    @conf_vars(
        {
            ("celery", "BROKER_URL"): "amqp://guest:guest@rabbitmq:5672//",
            ("celery", "SSL_ACTIVE"): "True",
            ("celery", "SSL_KEY"): "/path/to/key.pem",
            ("celery", "SSL_CERT"): "/path/to/cert.pem",
            ("celery", "SSL_CACERT"): "/path/to/ca.pem",
        }
    )
    def test_amqp_broker_url_still_builds_ssl_config(self):
        """Test that amqp:// (non-TLS) broker URLs still build SSL config correctly (no regression)."""
        import importlib
        import ssl

        importlib.reload(default_celery)

        config = default_celery.DEFAULT_CELERY_CONFIG
        assert "broker_use_ssl" in config
        broker_ssl = config["broker_use_ssl"]
        assert broker_ssl["keyfile"] == "/path/to/key.pem"
        assert broker_ssl["cert_reqs"] == ssl.CERT_REQUIRED

    @conf_vars(
        {
            ("celery", "BROKER_URL"): "rediss://redis:6380//",
            ("celery", "SSL_ACTIVE"): "True",
            ("celery", "SSL_KEY"): "/path/to/key.pem",
            ("celery", "SSL_CERT"): "/path/to/cert.pem",
            ("celery", "SSL_CACERT"): "/path/to/ca.pem",
        }
    )
    def test_redis_mutual_tls_builds_ssl_config(self):
        """Test mutual TLS: all three SSL keys produce correct broker_use_ssl for Redis."""
        import importlib
        import ssl

        importlib.reload(default_celery)

        config = default_celery.DEFAULT_CELERY_CONFIG
        assert "broker_use_ssl" in config
        broker_ssl = config["broker_use_ssl"]
        assert broker_ssl["ssl_keyfile"] == "/path/to/key.pem"
        assert broker_ssl["ssl_certfile"] == "/path/to/cert.pem"
        assert broker_ssl["ssl_ca_certs"] == "/path/to/ca.pem"
        assert broker_ssl["ssl_cert_reqs"] == ssl.CERT_REQUIRED

    @conf_vars(
        {
            ("celery", "BROKER_URL"): "amqps://guest:guest@rabbitmq:5671//",
            ("celery", "SSL_ACTIVE"): "True",
            ("celery", "SSL_CACERT"): "/path/to/ca.pem",
        }
    )
    def test_amqps_mutual_tls_missing_key_cert_raises(self):
        """Test that mutual TLS (default) raises error when SSL_KEY/SSL_CERT are missing."""
        import importlib

        with pytest.raises(ValueError, match="SSL_MUTUAL_TLS is True.*but SSL_KEY and/or SSL_CERT"):
            importlib.reload(default_celery)

    @conf_vars(
        {
            ("celery", "BROKER_URL"): "amqps://guest:guest@rabbitmq:5671//",
            ("celery", "SSL_ACTIVE"): "True",
            ("celery", "SSL_KEY"): "/path/to/key",
            ("celery", "SSL_CERT"): "/path/to/cert",
            ("celery", "SSL_CACERT"): "",
        }
    )
    def test_ssl_active_without_cacert_uses_system_cas(self):
        """Test that empty SSL_CACERT falls back to system CAs (ca_certs omitted from config)."""
        import importlib
        import ssl

        importlib.reload(default_celery)
        broker_ssl = default_celery.DEFAULT_CELERY_CONFIG["broker_use_ssl"]

        assert "ca_certs" not in broker_ssl
        assert broker_ssl["cert_reqs"] == ssl.CERT_REQUIRED

    @conf_vars(
        {
            ("celery", "BROKER_URL"): "amqps://guest:guest@rabbitmq:5671//",
            ("celery", "SSL_ACTIVE"): "False",
        }
    )
    def test_amqps_broker_url_no_ssl_when_inactive(self):
        """Test that amqps:// broker URLs don't get SSL config when SSL_ACTIVE is False."""
        import importlib

        importlib.reload(default_celery)

        config = default_celery.DEFAULT_CELERY_CONFIG
        assert "broker_use_ssl" not in config

    @conf_vars(
        {
            ("celery", "BROKER_URL"): "amqps://guest:guest@rabbitmq:5671//",
            ("celery", "SSL_ACTIVE"): "True",
            ("celery", "SSL_MUTUAL_TLS"): "False",
            ("celery", "SSL_CACERT"): "/path/to/ca.pem",
        }
    )
    def test_amqps_one_way_tls(self):
        """Test one-way TLS for AMQP: only ca_certs, no keyfile/certfile."""
        import importlib
        import ssl

        importlib.reload(default_celery)

        config = default_celery.DEFAULT_CELERY_CONFIG
        assert "broker_use_ssl" in config
        broker_ssl = config["broker_use_ssl"]
        assert broker_ssl["ca_certs"] == "/path/to/ca.pem"
        assert broker_ssl["cert_reqs"] == ssl.CERT_REQUIRED
        assert "keyfile" not in broker_ssl
        assert "certfile" not in broker_ssl

    @conf_vars(
        {
            ("celery", "BROKER_URL"): "rediss://redis:6380//",
            ("celery", "SSL_ACTIVE"): "True",
            ("celery", "SSL_MUTUAL_TLS"): "False",
            ("celery", "SSL_CACERT"): "/path/to/ca.pem",
        }
    )
    def test_redis_one_way_tls(self):
        """Test one-way TLS for Redis: only ssl_ca_certs, no ssl_keyfile/ssl_certfile."""
        import importlib
        import ssl

        importlib.reload(default_celery)

        config = default_celery.DEFAULT_CELERY_CONFIG
        assert "broker_use_ssl" in config
        broker_ssl = config["broker_use_ssl"]
        assert broker_ssl["ssl_ca_certs"] == "/path/to/ca.pem"
        assert broker_ssl["ssl_cert_reqs"] == ssl.CERT_REQUIRED
        assert "ssl_keyfile" not in broker_ssl
        assert "ssl_certfile" not in broker_ssl

    @conf_vars(
        {
            ("celery", "BROKER_URL"): "amqps://guest:guest@rabbitmq:5671//",
            ("celery", "SSL_ACTIVE"): "True",
            ("celery", "SSL_MUTUAL_TLS"): "False",
            ("celery", "SSL_KEY"): "/path/to/key.pem",
            ("celery", "SSL_CERT"): "/path/to/cert.pem",
            ("celery", "SSL_CACERT"): "/path/to/ca.pem",
        }
    )
    def test_one_way_tls_ignores_key_cert(self):
        """Test that SSL_KEY/SSL_CERT are ignored when SSL_MUTUAL_TLS is False."""
        import importlib
        import ssl

        importlib.reload(default_celery)

        config = default_celery.DEFAULT_CELERY_CONFIG
        assert "broker_use_ssl" in config
        broker_ssl = config["broker_use_ssl"]
        assert broker_ssl["ca_certs"] == "/path/to/ca.pem"
        assert broker_ssl["cert_reqs"] == ssl.CERT_REQUIRED
        assert "keyfile" not in broker_ssl
        assert "certfile" not in broker_ssl
