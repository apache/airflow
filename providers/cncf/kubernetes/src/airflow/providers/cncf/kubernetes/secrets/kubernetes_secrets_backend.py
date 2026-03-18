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
"""Objects relating to sourcing connections, variables, and configs from Kubernetes Secrets."""

from __future__ import annotations

import base64
from functools import cached_property
from pathlib import Path

from kubernetes.client import ApiClient, CoreV1Api
from kubernetes.config import load_incluster_config

from airflow.exceptions import AirflowException
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


class KubernetesSecretsBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieve connections, variables, and configs from Kubernetes Secrets using labels.

    This backend discovers secrets by querying Kubernetes labels, enabling integration
    with External Secrets Operator (ESO), Sealed Secrets, or any tool that creates
    Kubernetes secrets — regardless of the secret's name.

    Configurable via ``airflow.cfg``:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.KubernetesSecretsBackend
        backend_kwargs = {"namespace": "airflow", "connections_label": "airflow.apache.org/connection-id"}

    The secret must have a label whose key matches the configured label and whose value
    matches the requested identifier (conn_id, variable key, or config key). The actual
    secret value is read from the ``value`` key in the secret's data.

    Example Kubernetes secret for a connection named ``my_db``:

    .. code-block:: yaml

        apiVersion: v1
        kind: Secret
        metadata:
          name: anything
          labels:
            airflow.apache.org/connection-id: my_db
        data:
          value: <base64-encoded-connection-uri>

    **Authentication:** Uses ``kubernetes.config.load_incluster_config()`` directly
    for in-cluster authentication. Does not use KubernetesHook or any Airflow connection,
    avoiding circular dependencies since this IS the secrets backend.
    The namespace can be set explicitly via ``backend_kwargs``. If not set, it is
    auto-detected from the pod's service account metadata at
    ``/var/run/secrets/kubernetes.io/serviceaccount/namespace``. If auto-detection
    fails (e.g. ``automountServiceAccountToken`` is disabled), an error is raised.

    **Performance:** Queries use ``resource_version="0"`` so the Kubernetes API server
    serves results from its in-memory watch cache, making lookups very fast without
    requiring Airflow-side caching.

    :param namespace: Kubernetes namespace to query for secrets. If not set, the
        namespace is auto-detected from the pod's service account metadata. If
        auto-detection fails, an ``AirflowException`` is raised.
    :param connections_label: Label key used to discover connection secrets.
        If set to None, requests for connections will not be sent to Kubernetes.
    :param variables_label: Label key used to discover variable secrets.
        If set to None, requests for variables will not be sent to Kubernetes.
    :param config_label: Label key used to discover config secrets.
        If set to None, requests for configurations will not be sent to Kubernetes.
    :param connections_data_key: The data key in the Kubernetes secret that holds the
        connection value. Default: ``"value"``
    :param variables_data_key: The data key in the Kubernetes secret that holds the
        variable value. Default: ``"value"``
    :param config_data_key: The data key in the Kubernetes secret that holds the
        config value. Default: ``"value"``
    """

    DEFAULT_CONNECTIONS_LABEL = "airflow.apache.org/connection-id"
    DEFAULT_VARIABLES_LABEL = "airflow.apache.org/variable-key"
    DEFAULT_CONFIG_LABEL = "airflow.apache.org/config-key"
    SERVICE_ACCOUNT_NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

    def __init__(
        self,
        namespace: str | None = None,
        connections_label: str = DEFAULT_CONNECTIONS_LABEL,
        variables_label: str = DEFAULT_VARIABLES_LABEL,
        config_label: str = DEFAULT_CONFIG_LABEL,
        connections_data_key: str = "value",
        variables_data_key: str = "value",
        config_data_key: str = "value",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._namespace = namespace
        self.connections_label = connections_label
        self.variables_label = variables_label
        self.config_label = config_label
        self.connections_data_key = connections_data_key
        self.variables_data_key = variables_data_key
        self.config_data_key = config_data_key

    @cached_property
    def namespace(self) -> str:
        """Return the configured namespace, or auto-detect from service account metadata."""
        if self._namespace:
            return self._namespace
        try:
            return Path(self.SERVICE_ACCOUNT_NAMESPACE_PATH).read_text().strip()
        except FileNotFoundError:
            raise AirflowException(
                f"Could not auto-detect Kubernetes namespace from "
                f"{self.SERVICE_ACCOUNT_NAMESPACE_PATH}. "
                f"Is automountServiceAccountToken disabled for this pod? "
                f"Set the 'namespace' parameter explicitly in backend_kwargs."
            )

    @cached_property
    def client(self) -> CoreV1Api:
        """Lazy-init Kubernetes CoreV1Api client using in-cluster config directly."""
        load_incluster_config()
        return CoreV1Api(ApiClient())

    def get_conn_value(self, conn_id: str, team_name: str | None = None) -> str | None:
        """
        Get serialized representation of Connection from a Kubernetes secret.

        Multi-team isolation is not currently supported; ``team_name`` is accepted
        for API compatibility but ignored.

        :param conn_id: connection id
        :param team_name: Team name (unused — multi-team is not currently supported)
        """
        return self._get_secret(self.connections_label, conn_id, self.connections_data_key)

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """
        Get Airflow Variable from a Kubernetes secret.

        Multi-team isolation is not currently supported; ``team_name`` is accepted
        for API compatibility but ignored.

        :param key: Variable Key
        :param team_name: Team name (unused — multi-team is not currently supported)
        :return: Variable Value
        """
        return self._get_secret(self.variables_label, key, self.variables_data_key)

    def get_config(self, key: str) -> str | None:
        """
        Get Airflow Configuration from a Kubernetes secret.

        :param key: Configuration Option Key
        :return: Configuration Option Value
        """
        return self._get_secret(self.config_label, key, self.config_data_key)

    def _get_secret(self, label_key: str | None, label_value: str, data_key: str) -> str | None:
        """
        Get secret value from Kubernetes by label selector.

        Queries for secrets with a label ``{label_key}={label_value}`` using
        ``resource_version="0"`` for fast cached reads from the API server.

        :param label_key: The label key to search for. If None, returns None immediately
            (used to skip lookups when a label is not configured).
        :param label_value: The label value to match (e.g. conn_id or variable key)
        :param data_key: The key within the secret's data dict to read
        :return: Secret value or None if not found
        """
        if label_key is None:
            return None
        label_selector = f"{label_key}={label_value}"
        secret_list = self.client.list_namespaced_secret(
            self.namespace,
            label_selector=label_selector,
            resource_version="0",
        )
        if not secret_list.items:
            self.log.warning(
                "No secret found with label %s in namespace %s.",
                label_selector,
                self.namespace,
            )
            return None
        if len(secret_list.items) > 1:
            self.log.warning(
                "Multiple secrets found with label %s in namespace %s. Using the first one.",
                label_selector,
                self.namespace,
            )
        secret = secret_list.items[0]
        if secret.data is None or data_key not in secret.data:
            self.log.warning(
                "Secret '%s' does not have data key '%s'.",
                secret.metadata.name,
                data_key,
            )
            return None
        return base64.b64decode(secret.data[data_key]).decode("utf-8")
