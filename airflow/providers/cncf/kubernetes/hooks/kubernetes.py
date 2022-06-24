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
import tempfile
import warnings
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

from kubernetes import client, config, watch
from kubernetes.config import ConfigException

from airflow.compat.functools import cached_property
from airflow.kubernetes.kube_client import _disable_verify_ssl, _enable_tcp_keepalive

try:
    import airflow.utils.yaml as yaml
except ImportError:
    import yaml  # type: ignore[no-redef]

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


def _load_body_to_dict(body):
    try:
        body_dict = yaml.safe_load(body)
    except yaml.YAMLError as e:
        raise AirflowException(f"Exception when loading resource definition: {e}\n")
    return body_dict


class KubernetesHook(BaseHook):
    """
    Creates Kubernetes API connection.

    - use in cluster configuration by using ``extra__kubernetes__in_cluster`` in connection
    - use custom config by providing path to the file using ``extra__kubernetes__kube_config_path``
    - use custom configuration by providing content of kubeconfig file via
        ``extra__kubernetes__kube_config`` in connection
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

    conn_name_attr = 'kubernetes_conn_id'
    default_conn_name = 'kubernetes_default'
    conn_type = 'kubernetes'
    hook_name = 'Kubernetes Cluster Connection'

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, StringField

        return {
            "extra__kubernetes__in_cluster": BooleanField(lazy_gettext('In cluster configuration')),
            "extra__kubernetes__kube_config_path": StringField(
                lazy_gettext('Kube config path'), widget=BS3TextFieldWidget()
            ),
            "extra__kubernetes__kube_config": StringField(
                lazy_gettext('Kube config (JSON format)'), widget=BS3TextFieldWidget()
            ),
            "extra__kubernetes__namespace": StringField(
                lazy_gettext('Namespace'), widget=BS3TextFieldWidget()
            ),
            "extra__kubernetes__cluster_context": StringField(
                lazy_gettext('Cluster context'), widget=BS3TextFieldWidget()
            ),
            "extra__kubernetes__disable_verify_ssl": BooleanField(lazy_gettext('Disable SSL')),
            "extra__kubernetes__disable_tcp_keepalive": BooleanField(lazy_gettext('Disable TCP keepalive')),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['host', 'schema', 'login', 'password', 'port', 'extra'],
            "relabeling": {},
        }

    def __init__(
        self,
        conn_id: Optional[str] = default_conn_name,
        client_configuration: Optional[client.Configuration] = None,
        cluster_context: Optional[str] = None,
        config_file: Optional[str] = None,
        in_cluster: Optional[bool] = None,
        disable_verify_ssl: Optional[bool] = None,
        disable_tcp_keepalive: Optional[bool] = None,
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.client_configuration = client_configuration
        self.cluster_context = cluster_context
        self.config_file = config_file
        self.in_cluster = in_cluster
        self.disable_verify_ssl = disable_verify_ssl
        self.disable_tcp_keepalive = disable_tcp_keepalive

        # these params used for transition in KPO to K8s hook
        # for a deprecation period we will continue to consider k8s settings from airflow.cfg
        self._deprecated_core_disable_tcp_keepalive: Optional[bool] = None
        self._deprecated_core_disable_verify_ssl: Optional[bool] = None
        self._deprecated_core_in_cluster: Optional[bool] = None
        self._deprecated_core_cluster_context: Optional[str] = None
        self._deprecated_core_config_file: Optional[str] = None

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
        if field_name.startswith('extra_'):
            raise ValueError(
                f"Got prefixed name {field_name}; please remove the 'extra__kubernetes__' prefix "
                f"when using this method."
            )
        if field_name in self.conn_extras:
            return self.conn_extras[field_name] or None
        prefixed_name = f"extra__kubernetes__{field_name}"
        return self.conn_extras.get(prefixed_name) or None

    @staticmethod
    def _deprecation_warning_core_param(deprecation_warnings):
        settings_list_str = ''.join([f"\n\t{k}={v!r}" for k, v in deprecation_warnings])
        warnings.warn(
            f"\nApplying core Airflow settings from section [kubernetes] with the following keys:"
            f"{settings_list_str}\n"
            "In a future release, KubernetesPodOperator will no longer consider core\n"
            "Airflow settings; define an Airflow connection instead.",
            DeprecationWarning,
        )

    def get_conn(self) -> Any:
        """Returns kubernetes api session for use with requests"""
        in_cluster = self._coalesce_param(
            self.in_cluster, self.conn_extras.get("extra__kubernetes__in_cluster") or None
        )
        cluster_context = self._coalesce_param(
            self.cluster_context, self.conn_extras.get("extra__kubernetes__cluster_context") or None
        )
        kubeconfig_path = self._coalesce_param(
            self.config_file, self.conn_extras.get("extra__kubernetes__kube_config_path") or None
        )

        kubeconfig = self.conn_extras.get("extra__kubernetes__kube_config") or None
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

        # BEGIN apply settings from core kubernetes configuration
        # this section should be removed in next major release
        deprecation_warnings: List[Tuple[str, Any]] = []
        if disable_verify_ssl is None and self._deprecated_core_disable_verify_ssl is True:
            deprecation_warnings.append(('verify_ssl', False))
            disable_verify_ssl = self._deprecated_core_disable_verify_ssl
        # by default, hook will try in_cluster first. so we only need to
        # apply core airflow config and alert when False and in_cluster not otherwise set.
        if in_cluster is None and self._deprecated_core_in_cluster is False:
            deprecation_warnings.append(('in_cluster', self._deprecated_core_in_cluster))
            in_cluster = self._deprecated_core_in_cluster
        if not cluster_context and self._deprecated_core_cluster_context:
            deprecation_warnings.append(('cluster_context', self._deprecated_core_cluster_context))
            cluster_context = self._deprecated_core_cluster_context
        if not kubeconfig_path and self._deprecated_core_config_file:
            deprecation_warnings.append(('config_file', self._deprecated_core_config_file))
            kubeconfig_path = self._deprecated_core_config_file
        if disable_tcp_keepalive is None and self._deprecated_core_disable_tcp_keepalive is True:
            deprecation_warnings.append(('enable_tcp_keepalive', False))
            disable_tcp_keepalive = True
        if deprecation_warnings:
            self._deprecation_warning_core_param(deprecation_warnings)
        # END apply settings from core kubernetes configuration

        if disable_verify_ssl is True:
            _disable_verify_ssl()
        if disable_tcp_keepalive is not True:
            _enable_tcp_keepalive()

        if in_cluster:
            self.log.debug("loading kube_config from: in_cluster configuration")
            config.load_incluster_config()
            return client.ApiClient()

        if kubeconfig_path is not None:
            self.log.debug("loading kube_config from: %s", kubeconfig_path)
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
                config.load_kube_config(
                    config_file=temp_config.name,
                    client_configuration=self.client_configuration,
                    context=cluster_context,
                )
            return client.ApiClient()

        return self._get_default_client(cluster_context=cluster_context)

    def _get_default_client(self, *, cluster_context=None):
        # if we get here, then no configuration has been supplied
        # we should try in_cluster since that's most likely
        # but failing that just load assuming a kubeconfig file
        # in the default location
        try:
            config.load_incluster_config(client_configuration=self.client_configuration)
        except ConfigException:
            self.log.debug("loading kube_config from: default file")
            config.load_kube_config(
                client_configuration=self.client_configuration,
                context=cluster_context,
            )
        return client.ApiClient()

    @cached_property
    def api_client(self) -> Any:
        """Cached Kubernetes API client"""
        return self.get_conn()

    @cached_property
    def core_v1_client(self):
        return client.CoreV1Api(api_client=self.api_client)

    def create_custom_object(
        self, group: str, version: str, plural: str, body: Union[str, dict], namespace: Optional[str] = None
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
        if namespace is None:
            namespace = self.get_namespace()
        if isinstance(body, str):
            body_dict = _load_body_to_dict(body)
        else:
            body_dict = body
        try:
            api.delete_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                name=body_dict["metadata"]["name"],
            )
            self.log.warning("Deleted SparkApplication with the same name.")
        except client.rest.ApiException:
            self.log.info("SparkApp %s not found.", body_dict['metadata']['name'])

        try:
            response = api.create_namespaced_custom_object(
                group=group, version=version, namespace=namespace, plural=plural, body=body_dict
            )
            self.log.debug("Response: %s", response)
            return response
        except client.rest.ApiException as e:
            raise AirflowException(f"Exception when calling -> create_custom_object: {e}\n")

    def get_custom_object(
        self, group: str, version: str, plural: str, name: str, namespace: Optional[str] = None
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
        if namespace is None:
            namespace = self.get_namespace()
        try:
            response = api.get_namespaced_custom_object(
                group=group, version=version, namespace=namespace, plural=plural, name=name
            )
            return response
        except client.rest.ApiException as e:
            raise AirflowException(f"Exception when calling -> get_custom_object: {e}\n")

    def get_namespace(self) -> Optional[str]:
        """Returns the namespace that defined in the connection"""
        if self.conn_id:
            connection = self.get_connection(self.conn_id)
            extras = connection.extra_dejson
            namespace = extras.get("extra__kubernetes__namespace", "default")
            return namespace
        return None

    def get_pod_log_stream(
        self,
        pod_name: str,
        container: Optional[str] = "",
        namespace: Optional[str] = None,
    ) -> Tuple[watch.Watch, Generator[str, None, None]]:
        """
        Retrieves a log stream for a container in a kubernetes pod.

        :param pod_name: pod name
        :param container: container name
        :param namespace: kubernetes namespace
        """
        api = client.CoreV1Api(self.api_client)
        watcher = watch.Watch()
        return (
            watcher,
            watcher.stream(
                api.read_namespaced_pod_log,
                name=pod_name,
                container=container,
                namespace=namespace if namespace else self.get_namespace(),
            ),
        )

    def get_pod_logs(
        self,
        pod_name: str,
        container: Optional[str] = "",
        namespace: Optional[str] = None,
    ):
        """
        Retrieves a container's log from the specified pod.

        :param pod_name: pod name
        :param container: container name
        :param namespace: kubernetes namespace
        """
        api = client.CoreV1Api(self.api_client)
        return api.read_namespaced_pod_log(
            name=pod_name,
            container=container,
            _preload_content=False,
            namespace=namespace if namespace else self.get_namespace(),
        )


def _get_bool(val) -> Optional[bool]:
    """
    Converts val to bool if can be done with certainty.
    If we cannot infer intention we return None.
    """
    if isinstance(val, bool):
        return val
    elif isinstance(val, str):
        if val.strip().lower() == 'true':
            return True
        elif val.strip().lower() == 'false':
            return False
    return None
