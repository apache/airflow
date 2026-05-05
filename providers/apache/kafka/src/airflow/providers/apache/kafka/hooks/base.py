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

from functools import cached_property
from typing import Any

from confluent_kafka.admin import AdminClient

from airflow.providers.common.compat.module_loading import import_string
from airflow.providers.common.compat.sdk import BaseHook

# librdkafka config options whose values are callables. They can be provided as
# dotted-path strings (on the connection extra or in ``config_dict``) and are
# resolved to callables before the client is built.
CALLBACK_CONFIG_KEYS = ("error_cb", "throttle_cb", "stats_cb", "log_cb", "oauth_cb", "on_commit")


class KafkaBaseHook(BaseHook):
    """
    A base hook for interacting with Apache Kafka.

    The Airflow connection's extra JSON is merged with the ``config_dict``.
    Each ``config_dict`` key overrides the same key from the connection.
    When the connection is not available and ``config_dict`` contains the key
    ``bootstrap.servers``, the client is built from ``config_dict`` alone
    without requiring a connection.

    :param kafka_config_id: The connection object to use, defaults to "kafka_default"
    :param config_dict: Optional confluent-kafka client configuration
        (e.g. ``{"bootstrap.servers": "localhost:9092"}``). Each key overrides the same
        key from the connection's config. Callback options (``error_cb``, ``throttle_cb``,
        ``stats_cb``, ``log_cb``, ``oauth_cb``, ``on_commit``) may be callables or
        dotted-path strings, which are resolved before the client is built.
    """

    conn_name_attr = "kafka_config_id"
    default_conn_name = "kafka_default"
    conn_type = "kafka"
    hook_name = "Apache Kafka"

    def __init__(
        self,
        kafka_config_id=default_conn_name,
        config_dict: dict[str, Any] | None = None,
        *args,
        **kwargs,
    ):
        """Initialize our Base."""
        super().__init__()
        self.kafka_config_id = kafka_config_id
        self.config_dict = config_dict

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "login", "password", "port", "host"],
            "relabeling": {"extra": "Config Dict"},
            "placeholders": {
                "extra": '{"bootstrap.servers": "localhost:9092", "group.id": "my-group"}',
            },
        }

    def _get_client(self, config) -> Any:
        return AdminClient(config)

    def _get_config(self) -> dict[str, Any]:
        """Resolve the client config: the connection extras, with ``config_dict`` keys taking precedence."""
        config_dict = self.config_dict or {}
        try:
            config = dict(self.get_connection(self.kafka_config_id).extra_dejson)
        except Exception:
            # The connection lookup can fail for different reasons depending on the
            # component the hook runs in (missing connection, no DB/API access). When
            # ``config_dict`` carries the brokers the hook can run without a connection;
            # otherwise re-raise.
            if not config_dict.get("bootstrap.servers"):
                raise
            self.log.debug(
                "Kafka connection %r is not available; building the client from 'config_dict' only.",
                self.kafka_config_id,
            )
            config = {}
        config.update(config_dict)
        self._resolve_callbacks(config)
        return config

    def _resolve_callbacks(self, config: dict[str, Any]) -> None:
        """Resolve callback options provided as dotted-path strings into callables."""
        for key in CALLBACK_CONFIG_KEYS:
            value = config.get(key)
            if isinstance(value, str):
                config[key] = import_string(value)

    @cached_property
    def get_conn(self) -> Any:
        """Get the configuration object."""
        config = self._get_config()

        if not (config.get("bootstrap.servers", None)):
            raise ValueError("config['bootstrap.servers'] must be provided.")

        bootstrap_servers = config.get("bootstrap.servers")
        if (
            bootstrap_servers
            and bootstrap_servers.find("cloud.goog") != -1
            and bootstrap_servers.find("managedkafka") != -1
        ):
            try:
                from airflow.providers.google.cloud.hooks.managed_kafka import ManagedKafkaHook
            except ImportError:
                from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

                raise AirflowOptionalProviderFeatureException(
                    "Failed to import ManagedKafkaHook. For using this functionality google provider version "
                    ">= 14.1.0 should be pre-installed."
                )
            self.log.info("Adding token generation for Google Auth to the confluent configuration.")
            hook = ManagedKafkaHook()
            token = hook.get_confluent_token
            config.update({"oauth_cb": token})
        return self._get_client(config)

    def test_connection(self) -> tuple[bool, str]:
        """Test Connectivity from the UI."""
        try:
            # Use the same resolved config as the real clients, so callbacks configured
            # as dotted-path strings (e.g. oauth_cb) are exercised by the test as well.
            config = self._get_config()
            t = AdminClient(config).list_topics(timeout=10)
            if t:
                return True, "Connection successful."
        except Exception as e:
            return False, str(e)

        return False, "Failed to establish connection."
