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

import tempfile
import warnings
from typing import TYPE_CHECKING, Any, Generator

from kubernetes import client, config, watch
from kubernetes.config import ConfigException

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.kubernetes.kube_client import _disable_verify_ssl, _enable_tcp_keepalive
from airflow.utils import yaml


def _load_body_to_dict(body):
    try:
        body_dict = yaml.safe_load(body)
    except yaml.YAMLError as e:
        raise AirflowException(f"Exception when loading resource definition: {e}\n")
    return body_dict


class KubernetesHook(BaseHook):
    """
    Creates Kubernetes API connection.

    - use in cluster configuration by using extra field ``in_cluster`` in connection
    - use custom config by providing path to the file using extra field ``kube_config_path`` in connection
    - use custom configuration by providing content of kubeconfig file via
        extra field ``kube_config`` in connection
    - use default config by providing no extras

    This hook check for configuration option in the above order. Once an option is present it will
    use this configuration.

    .. seealso::
        For more information about Kubernetes connection:
        :doc:`/connections/kubernetes`

    :param conn_id: The :ref:`kubernetes connection <howto/connection:kubernetes>`
        to Kubernetes cluster.
    :param client_configuration: Optional dictionary of client configuration params.
        Passed on to kubernetes client.
    :param cluster_context: Optionally specify a context to use (e.g. if you have multiple
        in your kubeconfig.
    :param config_file: Path to kubeconfig file.
    :param in_cluster: Set to ``True`` if running from within a kubernetes cluster.
    :param disable_verify_ssl: Set to ``True`` if SSL verification should be disabled.
    :param disable_tcp_keepalive: Set to ``True`` if you want to disable keepalive logic.
    """

    conn_name_attr = "kubernetes_conn_id"
    default_conn_name = "kubernetes_default"
    conn_type = "kubernetes"
    hook_name = "Kubernetes Cluster Connection"

    DEFAULT_NAMESPACE = "default"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, StringField

        return {
            "in_cluster": BooleanField(lazy_gettext("In cluster configuration")),
            "kube_config_path": StringField(lazy_gettext("Kube config path"), widget=BS3TextFieldWidget()),
            "kube_config": StringField(
                lazy_gettext("Kube config (JSON format)"), widget=BS3TextFieldWidget()
            ),
            "namespace": StringField(lazy_gettext("Namespace"), widget=BS3TextFieldWidget()),
            "cluster_context": StringField(lazy_gettext("Cluster context"), widget=BS3TextFieldWidget()),
            "disable_verify_ssl": BooleanField(lazy_gettext("Disable SSL")),
            "disable_tcp_keepalive": BooleanField(lazy_gettext("Disable TCP keepalive")),
            "xcom_sidecar_container_image": StringField(
                lazy_gettext("XCom sidecar image"), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["host", "schema", "login", "password", "port", "extra"],
            "relabeling": {},
        }

    def __init__(
        self,
        conn_id: str | None = default_conn_name,
        client_configuration: client.Configuration | None = None,
        cluster_context: str | None = None,
        config_file: str | None = None,
        in_cluster: bool | None = None,
        disable_verify_ssl: bool | None = None,
        disable_tcp_keepalive: bool | None = None,
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.client_configuration = client_configuration
        self.cluster_context = cluster_context
        self.config_file = config_file
        self.in_cluster = in_cluster
        self.disable_verify_ssl = disable_verify_ssl
        self.disable_tcp_keepalive = disable_tcp_keepalive
        self._is_in_cluster: bool | None = None

    @staticmethod
    def _coalesce_param(*params):
        for param in params:
            if param is not None:
                return param

    @cached_property
    def conn_extras(self):
        if self.conn_id:
            connection = self.get_connection(self.conn_id)
            extras = connection.extra_dejson
        else:
            extras = {}
        return extras

    def _get_field(self, field_name):
        """
        Prior to Airflow 2.3, in order to make use of UI customizations for extra fields,
        we needed to store them with the prefix ``extra__kubernetes__``. This method
        handles the backcompat, i.e. if the extra dict contains prefixed fields.
        """
        if field_name.startswith("extra__"):
            raise ValueError(
                f"Got prefixed name {field_name}; please remove the 'extra__kubernetes__' prefix "
                f"when using this method."
            )
        if field_name in self.conn_extras:
            return self.conn_extras[field_name] or None
        prefixed_name = f"extra__kubernetes__{field_name}"
        return self.conn_extras.get(prefixed_name) or None

    def get_conn(self) -> client.ApiClient:
        """Returns kubernetes api session for use with requests"""
        in_cluster = self._coalesce_param(self.in_cluster, self._get_field("in_cluster"))
        cluster_context = self._coalesce_param(self.cluster_context, self._get_field("cluster_context"))
        kubeconfig_path = self._coalesce_param(self.config_file, self._get_field("kube_config_path"))
        kubeconfig = self._get_field("kube_config")
        num_selected_configuration = len([o for o in [in_cluster, kubeconfig, kubeconfig_path] if o])

        if num_selected_configuration > 1:
            raise AirflowException(
                "Invalid connection configuration. Options kube_config_path, "
                "kube_config, in_cluster are mutually exclusive. "
                "You can only use one option at a time."
            )

        disable_verify_ssl = self._coalesce_param(
            self.disable_verify_ssl, _get_bool(self._get_field("disable_verify_ssl"))
        )
        disable_tcp_keepalive = self._coalesce_param(
            self.disable_tcp_keepalive, _get_bool(self._get_field("disable_tcp_keepalive"))
        )

        if disable_verify_ssl is True:
            _disable_verify_ssl()
        if disable_tcp_keepalive is not True:
            _enable_tcp_keepalive()

        if in_cluster:
            self.log.debug("loading kube_config from: in_cluster configuration")
            self._is_in_cluster = True
            config.load_incluster_config()
            return client.ApiClient()

        if kubeconfig_path is not None:
            self.log.debug("loading kube_config from: %s", kubeconfig_path)
            self._is_in_cluster = False
            config.load_kube_config(
                config_file=kubeconfig_path,
                client_configuration=self.client_configuration,
                context=cluster_context,
            )
            return client.ApiClient()

        if kubeconfig is not None:
            with tempfile.NamedTemporaryFile() as temp_config:
                self.log.debug("loading kube_config from: connection kube_config")
                temp_config.write(kubeconfig.encode())
                temp_config.flush()
                self._is_in_cluster = False
                config.load_kube_config(
                    config_file=temp_config.name,
                    client_configuration=self.client_configuration,
                    context=cluster_context,
                )
            return client.ApiClient()

        return self._get_default_client(cluster_context=cluster_context)

    def _get_default_client(self, *, cluster_context: str | None = None) -> client.ApiClient:
        # if we get here, then no configuration has been supplied
        # we should try in_cluster since that's most likely
        # but failing that just load assuming a kubeconfig file
        # in the default location
        try:
            config.load_incluster_config(client_configuration=self.client_configuration)
            self._is_in_cluster = True
        except ConfigException:
            self.log.debug("loading kube_config from: default file")
            self._is_in_cluster = False
            config.load_kube_config(
                client_configuration=self.client_configuration,
                context=cluster_context,
            )
        return client.ApiClient()

    @property
    def is_in_cluster(self) -> bool:
        """Expose whether the hook is configured with ``load_incluster_config`` or not"""
        if self._is_in_cluster is not None:
            return self._is_in_cluster
        self.api_client  # so we can determine if we are in_cluster or not
        if TYPE_CHECKING:
            assert self._is_in_cluster is not None
        return self._is_in_cluster

    @cached_property
    def api_client(self) -> client.ApiClient:
        """Cached Kubernetes API client"""
        return self.get_conn()

    @cached_property
    def core_v1_client(self) -> client.CoreV1Api:
        return client.CoreV1Api(api_client=self.api_client)

    def create_custom_object(
        self, group: str, version: str, plural: str, body: str | dict, namespace: str | None = None
    ):
        """
        Creates custom resource definition object in Kubernetes
        :param group: api group
        :param version: api version
        :param plural: api plural
        :param body: crd object definition
        :param namespace: kubernetes namespace
        """
        api = client.CustomObjectsApi(self.api_client)
        namespace = namespace or self._get_namespace() or self.DEFAULT_NAMESPACE

        if isinstance(body, str):
            body_dict = _load_body_to_dict(body)
        else:
            body_dict = body

        # Attribute "name" is not mandatory if "generateName" is used instead
        if "name" in body_dict["metadata"]:
            try:
                api.delete_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=plural,
                    name=body_dict["metadata"]["name"],
                )

                self.log.warning("Deleted SparkApplication with the same name")
            except client.rest.ApiException:
                self.log.info("SparkApplication %s not found", body_dict["metadata"]["name"])

        try:
            response = api.create_namespaced_custom_object(
                group=group, version=version, namespace=namespace, plural=plural, body=body_dict
            )

            self.log.debug("Response: %s", response)
            return response
        except client.rest.ApiException as e:
            raise AirflowException(f"Exception when calling -> create_custom_object: {e}\n")

    def get_custom_object(
        self, group: str, version: str, plural: str, name: str, namespace: str | None = None
    ):
        """
        Get custom resource definition object from Kubernetes

        :param group: api group
        :param version: api version
        :param plural: api plural
        :param name: crd object name
        :param namespace: kubernetes namespace
        """
        api = client.CustomObjectsApi(self.api_client)
        namespace = namespace or self._get_namespace() or self.DEFAULT_NAMESPACE
        try:
            response = api.get_namespaced_custom_object(
                group=group, version=version, namespace=namespace, plural=plural, name=name
            )
            return response
        except client.rest.ApiException as e:
            raise AirflowException(f"Exception when calling -> get_custom_object: {e}\n")

    def get_namespace(self) -> str | None:
        """
        Returns the namespace defined in the connection or 'default'.

        TODO: in provider version 6.0, return None when namespace not defined in connection
        """
        namespace = self._get_namespace()
        if self.conn_id and not namespace:
            warnings.warn(
                "Airflow connection defined but namespace is not set; returning 'default'.  In "
                "cncf.kubernetes provider version 6.0 we will return None when namespace is "
                "not defined in the connection so that it's clear whether user intends 'default' or "
                "whether namespace is unset (which is required in order to apply precedence logic in "
                "KubernetesPodOperator).",
                DeprecationWarning,
            )
            return "default"
        return namespace

    def _get_namespace(self) -> str | None:
        """
        Returns the namespace that defined in the connection

        TODO: in provider version 6.0, get rid of this method and make it the behavior of get_namespace.
        """
        if self.conn_id:
            return self._get_field("namespace")
        return None

    def get_xcom_sidecar_container_image(self):
        """Returns the xcom sidecar image that defined in the connection"""
        return self._get_field("xcom_sidecar_container_image")

    def get_pod_log_stream(
        self,
        pod_name: str,
        container: str | None = "",
        namespace: str | None = None,
    ) -> tuple[watch.Watch, Generator[str, None, None]]:
        """
        Retrieves a log stream for a container in a kubernetes pod.

        :param pod_name: pod name
        :param container: container name
        :param namespace: kubernetes namespace
        """
        watcher = watch.Watch()
        return (
            watcher,
            watcher.stream(
                self.core_v1_client.read_namespaced_pod_log,
                name=pod_name,
                container=container,
                namespace=namespace or self._get_namespace() or self.DEFAULT_NAMESPACE,
            ),
        )

    def get_pod_logs(
        self,
        pod_name: str,
        container: str | None = "",
        namespace: str | None = None,
    ):
        """
        Retrieves a container's log from the specified pod.

        :param pod_name: pod name
        :param container: container name
        :param namespace: kubernetes namespace
        """
        return self.core_v1_client.read_namespaced_pod_log(
            name=pod_name,
            container=container,
            _preload_content=False,
            namespace=namespace or self._get_namespace() or self.DEFAULT_NAMESPACE,
        )


def _get_bool(val) -> bool | None:
    """
    Converts val to bool if can be done with certainty.
    If we cannot infer intention we return None.
    """
    if isinstance(val, bool):
        return val
    elif isinstance(val, str):
        if val.strip().lower() == "true":
            return True
        elif val.strip().lower() == "false":
            return False
    return None
