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
from airflow.providers.common.compat.sdk import BaseHook, conf

# librdkafka config options whose values are callables. They can be provided as dotted-path
# strings on the connection extra and are resolved to callables before the client is built.
CALLBACK_CONFIG_KEYS = ("error_cb", "throttle_cb", "stats_cb", "log_cb", "oauth_cb", "on_commit")


KAFKA_COMMON_CONFIG_SECTION = "apache_kafka"
# Configuration allowlist for callbacks that are allowed to be resolved from a connection extra.
CALLBACK_ALLOWLIST_CONFIG_OPTION = "callback_allowlist"


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

    @staticmethod
    def _get_callback_allowlist() -> frozenset[str]:
        conf_value = (
            conf.get(KAFKA_COMMON_CONFIG_SECTION, CALLBACK_ALLOWLIST_CONFIG_OPTION, fallback="") or ""
        )
        return frozenset(cb_path.strip() for cb_path in conf_value.split(",") if cb_path.strip())

    def _resolve_callbacks(self, config: dict[str, Any]) -> None:
        """
        Resolve callback options provided as dotted-path strings into callables.

        A callback is resolved only when its full importable path is listed in
        the ``[apache_kafka] callback_allowlist`` configuration. This is enforced
        for security reasons, to prevent malicious callbacks from being executed.
        """
        allowlist: frozenset[str] | None = None
        for key in CALLBACK_CONFIG_KEYS:
            value = config.get(key)
            if not isinstance(value, str):
                continue
            if allowlist is None:
                # Get the allowlist from the config only once, if it hasn't
                # already been initialized by a previous iteration.
                allowlist = self._get_callback_allowlist()
            if not allowlist:
                # If the allowlist is empty, break the iteration immediately by raising an error.
                self.log.warning(
                    "Kafka connection %r requests callback %s=%r, but [%s] %s is empty. Add the "
                    "full importable path of each callback you trust (e.g. 'my_pkg.auth.oauth_cb') "
                    "to allow string-valued callbacks.",
                    self.kafka_config_id,
                    key,
                    value,
                    KAFKA_COMMON_CONFIG_SECTION,
                    CALLBACK_ALLOWLIST_CONFIG_OPTION,
                )
                raise ValueError(
                    f"Refusing to resolve Kafka callback {key}={value!r}: the "
                    f"[{KAFKA_COMMON_CONFIG_SECTION}] {CALLBACK_ALLOWLIST_CONFIG_OPTION} is empty."
                )
            if value not in allowlist:
                raise ValueError(
                    f"Refusing to resolve Kafka callback {key}={value!r}: it is not in the "
                    f"[{KAFKA_COMMON_CONFIG_SECTION}] {CALLBACK_ALLOWLIST_CONFIG_OPTION} "
                    f"({', '.join(sorted(allowlist))})."
                )
            config[key] = import_string(value)

    def _build_config(self) -> dict[str, Any]:
        """
        Build the confluent-kafka configuration for this connection.

        Resolves callback options provided as dotted-path strings and injects the
        managed OAuth token callback (Google Managed Kafka or Amazon MSK IAM) when
        applicable, so that establishing a connection and testing it always use an
        identical configuration.
        """
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
        return config

    @cached_property
    def get_conn(self) -> Any:
        """Get the configuration object."""
        return self._get_client(self._build_config())

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
            # Build the config exactly as a real connection would, so resolved
            # dotted-path callbacks and the managed OAuth token callback (Google
            # Managed Kafka or Amazon MSK IAM) are exercised by the UI test too.
            config = self._build_config()
            t = AdminClient(config).list_topics(timeout=10)
            if t:
                return True, "Connection successful."
        except Exception as e:
            return False, str(e)

        return False, "Failed to establish connection."
