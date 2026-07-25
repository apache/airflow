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

import re
from functools import cached_property, partial
from typing import Any

from confluent_kafka.admin import AdminClient

from airflow.providers.common.compat.module_loading import import_string
from airflow.providers.common.compat.sdk import BaseHook

# librdkafka config options whose values are callables. They can be provided as dotted-path
# strings on the connection extra and are resolved to callables before the client is built.
CALLBACK_CONFIG_KEYS = ("error_cb", "throttle_cb", "stats_cb", "log_cb", "oauth_cb", "on_commit")

# Amazon MSK bootstrap servers follow a predictable naming scheme, e.g.
#   b-1.demo.abcde1.c2.kafka.us-east-1.amazonaws.com:9098            (provisioned)
#   boot-abcde1.c2.kafka-serverless.us-east-1.amazonaws.com:9098     (serverless)
# China regions use the ``.amazonaws.com.cn`` suffix. The region is captured so it
# can be forwarded to the MSK IAM token signer.
MSK_BOOTSTRAP_SERVERS_REGEX = re.compile(
    r"\.kafka(?:-serverless)?\.(?P<region>[a-z0-9-]+)\.amazonaws\.com(?:\.cn)?(?::\d+)?(?=$|[,\s])",
    re.IGNORECASE,
)


def _msk_iam_oauth_cb(region: str, config_str: str) -> tuple[str, float]:
    """
    Generate an OAUTHBEARER token for Amazon MSK IAM authentication.

    This is used as the ``oauth_cb`` callback for ``confluent-kafka``. The library
    passes the value of ``sasl.oauthbearer.config`` as ``config_str``; it is not
    needed to sign an MSK IAM token, so it is ignored.

    :param region: The AWS region of the MSK cluster.
    :param config_str: The ``sasl.oauthbearer.config`` value passed by librdkafka.
    """
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
    # The signer returns the expiry as milliseconds since the epoch while
    # confluent-kafka expects seconds since the epoch.
    return token, expiry_ms / 1000


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

    def _resolve_callbacks(self, config: dict[str, Any]) -> None:
        """Resolve callback options provided as dotted-path strings into callables."""
        for key in CALLBACK_CONFIG_KEYS:
            value = config.get(key)
            if isinstance(value, str):
                config[key] = import_string(value)

    @cached_property
    def get_conn(self) -> Any:
        """Get the configuration object."""
        config = self.get_connection(self.kafka_config_id).extra_dejson
        self._resolve_callbacks(config)

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
        else:
            self._maybe_add_msk_iam_oauth(config, bootstrap_servers)
        return self._get_client(config)

    def _maybe_add_msk_iam_oauth(self, config: dict[str, Any], bootstrap_servers: str | None) -> None:
        """
        Inject an OAUTHBEARER token callback for Amazon MSK IAM authentication.

        The callback is only added when the bootstrap servers point at an Amazon MSK
        cluster and the connection is configured to use the ``OAUTHBEARER`` SASL
        mechanism. An explicit user-provided ``oauth_cb`` is never overwritten.
        """
        if not bootstrap_servers:
            return

        sasl_mechanism = config.get("sasl.mechanism") or config.get("sasl.mechanisms")
        if sasl_mechanism != "OAUTHBEARER":
            return

        match = MSK_BOOTSTRAP_SERVERS_REGEX.search(bootstrap_servers)
        if not match:
            return

        if "oauth_cb" in config:
            # Respect an explicit callback provided by the user.
            return

        try:
            from aws_msk_iam_sasl_signer import MSKAuthTokenProvider  # noqa: F401
        except ImportError:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "Failed to import aws_msk_iam_sasl_signer. To use Amazon MSK IAM authentication "
                "install the 'msk' extra: pip install apache-airflow-providers-apache-kafka[msk]"
            )

        region = match.group("region").lower()
        self.log.info(
            "Adding token generation for Amazon MSK IAM (region %s) to the confluent configuration.",
            region,
        )
        config.update({"oauth_cb": partial(_msk_iam_oauth_cb, region)})

    def test_connection(self) -> tuple[bool, str]:
        """Test Connectivity from the UI."""
        try:
            config = self.get_connection(self.kafka_config_id).extra_dejson
            # Resolve callbacks so that configured dotted-path strings
            # (e.g. oauth_cb) are exercised by the test as well.
            self._resolve_callbacks(config)
            t = AdminClient(config).list_topics(timeout=10)
            if t:
                return True, "Connection successful."
        except Exception as e:
            return False, str(e)

        return False, "Failed to establish connection."
