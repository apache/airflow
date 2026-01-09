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

from airflow.providers.common.compat.sdk import BaseHook


class KafkaBaseHook(BaseHook):
    """
    A base hook for interacting with Apache Kafka.

    :param kafka_config_id: The connection object to use, defaults to "kafka_default"
    """

    conn_name_attr = "kafka_config_id"
    default_conn_name = "kafka_default"
    conn_type = "kafka"
    hook_name = "Apache Kafka"

    def __init__(self, kafka_config_id=default_conn_name, *args, **kwargs):
        """Initialize our Base."""
        super().__init__()
        self.kafka_config_id = kafka_config_id

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

    @cached_property
    def get_conn(self) -> Any:
        """Get the configuration object."""
        config = self.get_connection(self.kafka_config_id).extra_dejson

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
                from airflow.exceptions import AirflowOptionalProviderFeatureException

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
            config = self.get_connection(self.kafka_config_id).extra_dejson
            t = AdminClient(config).list_topics(timeout=10)
            if t:
                return True, "Connection successful."
        except Exception as e:
            return False, str(e)

        return False, "Failed to establish connection."
