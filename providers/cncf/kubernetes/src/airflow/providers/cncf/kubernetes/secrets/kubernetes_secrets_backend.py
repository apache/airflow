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
from kubernetes.client.exceptions import ApiException
from kubernetes.config import load_incluster_config

from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


class KubernetesSecretsBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieve connections, variables, and configs from Kubernetes Secrets.

    This backend reads secrets using a naming convention, enabling integration with
    External Secrets Operator (ESO) or any tool that creates Kubernetes secrets with
    a predictable naming scheme.

    Configurable via ``airflow.cfg`` like so:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.cncf.kubernetes.secrets.kubernetes_secrets_backend.KubernetesSecretsBackend
        backend_kwargs = {"connections_prefix": "airflow-connections"}

    For example, if the Kubernetes secret name is ``airflow-connections-my-db`` with a data key
    ``value`` containing a connection URI, this would be accessible if you provide
    ``{"connections_prefix": "airflow-connections"}`` and request conn_id ``my_db``.

    The secret name is built as ``{prefix}-{key}`` where underscores in the key are
    replaced with hyphens to conform to Kubernetes DNS naming requirements.

    **Authentication:** Uses ``kubernetes.config.load_incluster_config()`` directly
    for in-cluster authentication. Does not use KubernetesHook or any Airflow connection,
    avoiding circular dependencies since this IS the secrets backend.
    The namespace is auto-detected from the pod's service account metadata.

    :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
        If set to None, requests for connections will not be sent to Kubernetes.
    :param variables_prefix: Specifies the prefix of the secret to read to get Variables.
        If set to None, requests for variables will not be sent to Kubernetes.
    :param config_prefix: Specifies the prefix of the secret to read to get Configurations.
        If set to None, requests for configurations will not be sent to Kubernetes.
    :param connections_data_key: The data key in the Kubernetes secret that holds the
        connection value. Default: ``"value"``
    :param variables_data_key: The data key in the Kubernetes secret that holds the
        variable value. Default: ``"value"``
    :param config_data_key: The data key in the Kubernetes secret that holds the
        config value. Default: ``"value"``
    """

    def __init__(
        self,
        connections_prefix: str | None = "airflow-connections",
        variables_prefix: str | None = "airflow-variables",
        config_prefix: str | None = "airflow-config",
        connections_data_key: str = "value",
        variables_data_key: str = "value",
        config_data_key: str = "value",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.connections_prefix = connections_prefix
        self.variables_prefix = variables_prefix
        self.config_prefix = config_prefix
        self.connections_data_key = connections_data_key
        self.variables_data_key = variables_data_key
        self.config_data_key = config_data_key

    @cached_property
    def namespace(self) -> str:
        """Auto-detect namespace from the pod's service account metadata, falling back to 'default'."""
        try:
            return Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read_text().strip()
        except FileNotFoundError:
            return "default"

    @cached_property
    def client(self) -> CoreV1Api:
        """Lazy-init Kubernetes CoreV1Api client using in-cluster config directly."""
        load_incluster_config()
        return CoreV1Api(ApiClient())

    def get_conn_value(self, conn_id: str, team_name: str | None = None) -> str | None:
        """
        Get serialized representation of Connection from a Kubernetes secret.

        :param conn_id: connection id
        :param team_name: Team name associated to the task trying to access the connection (if any)
        """
        if self.connections_prefix is None:
            return None
        return self._get_secret(self.connections_prefix, conn_id, self.connections_data_key)

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """
        Get Airflow Variable from a Kubernetes secret.

        :param key: Variable Key
        :param team_name: Team name associated to the task trying to access the variable (if any)
        :return: Variable Value
        """
        if self.variables_prefix is None:
            return None
        return self._get_secret(self.variables_prefix, key, self.variables_data_key)

    def get_config(self, key: str) -> str | None:
        """
        Get Airflow Configuration from a Kubernetes secret.

        :param key: Configuration Option Key
        :return: Configuration Option Value
        """
        if self.config_prefix is None:
            return None
        return self._get_secret(self.config_prefix, key, self.config_data_key)

    def _get_secret(self, prefix: str, key: str, data_key: str) -> str | None:
        """
        Get secret value from Kubernetes.

        Builds the secret name as ``{prefix}-{key}``, sanitizes it for K8s DNS
        compatibility (underscores to hyphens), reads the secret, and returns the
        base64-decoded value for the specified data key.

        :param prefix: Prefix for the secret name
        :param key: Secret key (e.g. conn_id or variable key)
        :param data_key: The key within the secret's data dict to read
        :return: Secret value or None if not found
        """
        secret_name = self.build_path(prefix, key, "-")
        # Sanitize for Kubernetes DNS naming: underscores to hyphens, lowercase
        secret_name = secret_name.replace("_", "-").lower()
        try:
            secret = self.client.read_namespaced_secret(secret_name, self.namespace)
        except ApiException as e:
            if e.status == 404:
                self.log.debug("Secret %s not found in namespace %s.", secret_name, self.namespace)
                return None
            raise
        if secret.data is None or data_key not in secret.data:
            self.log.debug(
                "Secret %s does not have data key '%s'.",
                secret_name,
                data_key,
            )
            return None
        return base64.b64decode(secret.data[data_key]).decode("utf-8")
