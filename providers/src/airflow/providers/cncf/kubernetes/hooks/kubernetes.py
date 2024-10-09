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

import asyncio
import contextlib
import json
import tempfile
from functools import cached_property
from time import sleep
from typing import TYPE_CHECKING, Any, Generator

import aiofiles
import tenacity
from asgiref.sync import sync_to_async
from kubernetes import client, config, watch
from kubernetes.config import ConfigException
from kubernetes_asyncio import client as async_client, config as async_config
from urllib3.exceptions import HTTPError

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.kube_client import _disable_verify_ssl, _enable_tcp_keepalive
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import should_retry_creation
from airflow.providers.cncf.kubernetes.utils.pod_manager import (
    PodOperatorHookProtocol,
    container_is_completed,
    container_is_running,
)
from airflow.utils import yaml

if TYPE_CHECKING:
    from kubernetes.client import V1JobList
    from kubernetes.client.models import V1Deployment, V1Job, V1Pod

LOADING_KUBE_CONFIG_FILE_RESOURCE = "Loading Kubernetes configuration file kube_config from {}..."

JOB_FINAL_STATUS_CONDITION_TYPES = {
    "Complete",
    "Failed",
}

JOB_STATUS_CONDITION_TYPES = JOB_FINAL_STATUS_CONDITION_TYPES | {"Suspended"}


def _load_body_to_dict(body: str) -> dict:
    try:
        body_dict = yaml.safe_load(body)
    except yaml.YAMLError as e:
        raise AirflowException(f"Exception when loading resource definition: {e}\n")
    return body_dict


class KubernetesHook(BaseHook, PodOperatorHookProtocol):
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

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, PasswordField, StringField

        return {
            "in_cluster": BooleanField(lazy_gettext("In cluster configuration")),
            "kube_config_path": StringField(lazy_gettext("Kube config path"), widget=BS3TextFieldWidget()),
            "kube_config": PasswordField(
                lazy_gettext("Kube config (JSON format)"), widget=BS3PasswordFieldWidget()
            ),
            "namespace": StringField(lazy_gettext("Namespace"), widget=BS3TextFieldWidget()),
            "cluster_context": StringField(lazy_gettext("Cluster context"), widget=BS3TextFieldWidget()),
            "disable_verify_ssl": BooleanField(lazy_gettext("Disable SSL")),
            "disable_tcp_keepalive": BooleanField(lazy_gettext("Disable TCP keepalive")),
            "xcom_sidecar_container_image": StringField(
                lazy_gettext("XCom sidecar image"), widget=BS3TextFieldWidget()
            ),
            "xcom_sidecar_container_resources": StringField(
                lazy_gettext("XCom sidecar resources (JSON format)"), widget=BS3TextFieldWidget()
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
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

    @classmethod
    def get_connection(cls, conn_id: str) -> Connection:
        """
        Return requested connection.

        If missing and conn_id is "kubernetes_default", will return empty connection so that hook will
        default to cluster-derived credentials.
        """
        try:
            return super().get_connection(conn_id)
        except AirflowNotFoundException:
            if conn_id == cls.default_conn_name:
                return Connection(conn_id=cls.default_conn_name)
            else:
                raise

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
        Handle backcompat for extra fields.

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
        """Return kubernetes api session for use with requests."""
        in_cluster = self._coalesce_param(self.in_cluster, self._get_field("in_cluster"))
        cluster_context = self._coalesce_param(self.cluster_context, self._get_field("cluster_context"))
        kubeconfig_path = self._coalesce_param(self.config_file, self._get_field("kube_config_path"))
        kubeconfig = self._get_field("kube_config")
        num_selected_configuration = sum(1 for o in [in_cluster, kubeconfig, kubeconfig_path] if o)

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
                if isinstance(kubeconfig, dict):
                    kubeconfig = json.dumps(kubeconfig)
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
        """Expose whether the hook is configured with ``load_incluster_config`` or not."""
        if self._is_in_cluster is not None:
            return self._is_in_cluster
        self.api_client  # so we can determine if we are in_cluster or not
        if TYPE_CHECKING:
            assert self._is_in_cluster is not None
        return self._is_in_cluster

    @cached_property
    def api_client(self) -> client.ApiClient:
        """Cached Kubernetes API client."""
        return self.get_conn()

    @cached_property
    def core_v1_client(self) -> client.CoreV1Api:
        return client.CoreV1Api(api_client=self.api_client)

    @cached_property
    def apps_v1_client(self) -> client.AppsV1Api:
        return client.AppsV1Api(api_client=self.api_client)

    @cached_property
    def custom_object_client(self) -> client.CustomObjectsApi:
        return client.CustomObjectsApi(api_client=self.api_client)

    @cached_property
    def batch_v1_client(self) -> client.BatchV1Api:
        return client.BatchV1Api(api_client=self.api_client)

    def create_custom_object(
        self, group: str, version: str, plural: str, body: str | dict, namespace: str | None = None
    ):
        """
        Create custom resource definition object in Kubernetes.

        :param group: api group
        :param version: api version
        :param plural: api plural
        :param body: crd object definition
        :param namespace: kubernetes namespace
        """
        api: client.CustomObjectsApi = self.custom_object_client

        if isinstance(body, str):
            body_dict = _load_body_to_dict(body)
        else:
            body_dict = body

        response = api.create_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace or self.get_namespace() or self.DEFAULT_NAMESPACE,
            plural=plural,
            body=body_dict,
        )

        self.log.debug("Response: %s", response)
        return response

    def get_custom_object(
        self, group: str, version: str, plural: str, name: str, namespace: str | None = None
    ):
        """
        Get custom resource definition object from Kubernetes.

        :param group: api group
        :param version: api version
        :param plural: api plural
        :param name: crd object name
        :param namespace: kubernetes namespace
        """
        api = client.CustomObjectsApi(self.api_client)
        response = api.get_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace or self.get_namespace() or self.DEFAULT_NAMESPACE,
            plural=plural,
            name=name,
        )
        return response

    def delete_custom_object(
        self, group: str, version: str, plural: str, name: str, namespace: str | None = None, **kwargs
    ):
        """
        Delete custom resource definition object from Kubernetes.

        :param group: api group
        :param version: api version
        :param plural: api plural
        :param name: crd object name
        :param namespace: kubernetes namespace
        """
        api = client.CustomObjectsApi(self.api_client)
        return api.delete_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace or self.get_namespace() or self.DEFAULT_NAMESPACE,
            plural=plural,
            name=name,
            **kwargs,
        )

    def get_namespace(self) -> str | None:
        """Return the namespace that defined in the connection."""
        if self.conn_id:
            return self._get_field("namespace")
        return None

    def get_xcom_sidecar_container_image(self):
        """Return the xcom sidecar image that defined in the connection."""
        return self._get_field("xcom_sidecar_container_image")

    def get_xcom_sidecar_container_resources(self):
        """Return the xcom sidecar resources that defined in the connection."""
        field = self._get_field("xcom_sidecar_container_resources")
        if not field:
            return None
        return json.loads(field)

    def get_pod_log_stream(
        self,
        pod_name: str,
        container: str | None = "",
        namespace: str | None = None,
    ) -> tuple[watch.Watch, Generator[str, None, None]]:
        """
        Retrieve a log stream for a container in a kubernetes pod.

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
                namespace=namespace or self.get_namespace() or self.DEFAULT_NAMESPACE,
            ),
        )

    def get_pod_logs(
        self,
        pod_name: str,
        container: str | None = "",
        namespace: str | None = None,
    ):
        """
        Retrieve a container's log from the specified pod.

        :param pod_name: pod name
        :param container: container name
        :param namespace: kubernetes namespace
        """
        return self.core_v1_client.read_namespaced_pod_log(
            name=pod_name,
            container=container,
            _preload_content=False,
            namespace=namespace or self.get_namespace() or self.DEFAULT_NAMESPACE,
        )

    def get_pod(self, name: str, namespace: str) -> V1Pod:
        """Read pod object from kubernetes API."""
        return self.core_v1_client.read_namespaced_pod(
            name=name,
            namespace=namespace,
        )

    def get_namespaced_pod_list(
        self,
        label_selector: str | None = "",
        namespace: str | None = None,
        watch: bool = False,
        **kwargs,
    ):
        """
        Retrieve a list of Kind pod which belong default kubernetes namespace.

        :param label_selector: A selector to restrict the list of returned objects by their labels
        :param namespace: kubernetes namespace
        :param watch: Watch for changes to the described resources and return them as a stream
        """
        return self.core_v1_client.list_namespaced_pod(
            namespace=namespace or self.get_namespace() or self.DEFAULT_NAMESPACE,
            watch=watch,
            label_selector=label_selector,
            _preload_content=False,
            **kwargs,
        )

    def get_deployment_status(
        self,
        name: str,
        namespace: str = "default",
        **kwargs,
    ) -> V1Deployment:
        """
        Get status of existing Deployment.

        :param name: Name of Deployment to retrieve
        :param namespace: Deployment namespace
        """
        try:
            return self.apps_v1_client.read_namespaced_deployment_status(
                name=name, namespace=namespace, pretty=True, **kwargs
            )
        except Exception as exc:
            raise exc

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_random_exponential(),
        reraise=True,
        retry=tenacity.retry_if_exception(should_retry_creation),
    )
    def create_job(
        self,
        job: V1Job,
        **kwargs,
    ) -> V1Job:
        """
        Run Job.

        :param job: A kubernetes Job object
        """
        sanitized_job = self.batch_v1_client.api_client.sanitize_for_serialization(job)
        json_job = json.dumps(sanitized_job, indent=2)

        self.log.debug("Job Creation Request: \n%s", json_job)
        try:
            resp = self.batch_v1_client.create_namespaced_job(
                body=sanitized_job, namespace=job.metadata.namespace, **kwargs
            )
            self.log.debug("Job Creation Response: %s", resp)
        except Exception as e:
            self.log.exception(
                "Exception when attempting to create Namespaced Job: %s", str(json_job).replace("\n", " ")
            )
            raise e
        return resp

    def get_job(self, job_name: str, namespace: str) -> V1Job:
        """
        Get Job of specified name and namespace.

        :param job_name: Name of Job to fetch.
        :param namespace: Namespace of the Job.
        :return: Job object
        """
        return self.batch_v1_client.read_namespaced_job(name=job_name, namespace=namespace, pretty=True)

    def get_job_status(self, job_name: str, namespace: str) -> V1Job:
        """
        Get job with status of specified name and namespace.

        :param job_name: Name of Job to fetch.
        :param namespace: Namespace of the Job.
        :return: Job object
        """
        return self.batch_v1_client.read_namespaced_job_status(
            name=job_name, namespace=namespace, pretty=True
        )

    def wait_until_job_complete(self, job_name: str, namespace: str, job_poll_interval: float = 10) -> V1Job:
        """
        Block job of specified name and namespace until it is complete or failed.

        :param job_name: Name of Job to fetch.
        :param namespace: Namespace of the Job.
        :param job_poll_interval: Interval in seconds between polling the job status
        :return: Job object
        """
        while True:
            self.log.info("Requesting status for the job '%s' ", job_name)
            job: V1Job = self.get_job_status(job_name=job_name, namespace=namespace)
            if self.is_job_complete(job=job):
                return job
            self.log.info("The job '%s' is incomplete. Sleeping for %i sec.", job_name, job_poll_interval)
            sleep(job_poll_interval)

    def list_jobs_all_namespaces(self) -> V1JobList:
        """
        Get list of Jobs from all namespaces.

        :return: V1JobList object
        """
        return self.batch_v1_client.list_job_for_all_namespaces(pretty=True)

    def list_jobs_from_namespace(self, namespace: str) -> V1JobList:
        """
        Get list of Jobs from dedicated namespace.

        :param namespace: Namespace of the Job.
        :return: V1JobList object
        """
        return self.batch_v1_client.list_namespaced_job(namespace=namespace, pretty=True)

    def is_job_complete(self, job: V1Job) -> bool:
        """
        Check whether the given job is complete (with success or fail).

        :return: Boolean indicating that the given job is complete.
        """
        if status := job.status:
            if conditions := status.conditions:
                if final_condition_types := list(
                    c for c in conditions if c.type in JOB_FINAL_STATUS_CONDITION_TYPES and c.status
                ):
                    s = "s" if len(final_condition_types) > 1 else ""
                    self.log.info(
                        "The job '%s' state%s: %s",
                        job.metadata.name,
                        s,
                        ", ".join(f"{c.type} at {c.last_transition_time}" for c in final_condition_types),
                    )
                    return True
        return False

    @staticmethod
    def is_job_failed(job: V1Job) -> str | bool:
        """
        Check whether the given job is failed.

        :return: Error message if the job is failed, and False otherwise.
        """
        if status := job.status:
            conditions = status.conditions or []
            if fail_condition := next((c for c in conditions if c.type == "Failed" and c.status), None):
                return fail_condition.reason
        return False

    @staticmethod
    def is_job_successful(job: V1Job) -> str | bool:
        """
        Check whether the given job is completed successfully..

        :return: Error message if the job is failed, and False otherwise.
        """
        if status := job.status:
            conditions = status.conditions or []
            return bool(next((c for c in conditions if c.type == "Complete" and c.status), None))
        return False

    def patch_namespaced_job(self, job_name: str, namespace: str, body: object) -> V1Job:
        """
        Update the specified Job.

        :param job_name: name of the Job
        :param namespace: the namespace to run within kubernetes
        :param body: json object with parameters for update
        """
        return self.batch_v1_client.patch_namespaced_job(
            name=job_name,
            namespace=namespace,
            body=body,
        )


def _get_bool(val) -> bool | None:
    """Convert val to bool if can be done with certainty; if we cannot infer intention we return None."""
    if isinstance(val, bool):
        return val
    elif isinstance(val, str):
        if val.strip().lower() == "true":
            return True
        elif val.strip().lower() == "false":
            return False
    return None


class AsyncKubernetesHook(KubernetesHook):
    """Hook to use Kubernetes SDK asynchronously."""

    def __init__(self, config_dict: dict | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.config_dict = config_dict
        self._extras: dict | None = None

    async def _load_config(self):
        """Return Kubernetes API session for use with requests."""
        in_cluster = self._coalesce_param(self.in_cluster, await self._get_field("in_cluster"))
        cluster_context = self._coalesce_param(self.cluster_context, await self._get_field("cluster_context"))
        kubeconfig = await self._get_field("kube_config")

        num_selected_configuration = sum(1 for o in [in_cluster, kubeconfig, self.config_dict] if o)

        if num_selected_configuration > 1:
            raise AirflowException(
                "Invalid connection configuration. Options kube_config_path, "
                "kube_config, in_cluster are mutually exclusive. "
                "You can only use one option at a time."
            )

        if in_cluster:
            self.log.debug(LOADING_KUBE_CONFIG_FILE_RESOURCE.format("within a pod"))
            self._is_in_cluster = True
            async_config.load_incluster_config()
            return async_client.ApiClient()

        if self.config_dict:
            self.log.debug(LOADING_KUBE_CONFIG_FILE_RESOURCE.format("config dictionary"))
            self._is_in_cluster = False
            await async_config.load_kube_config_from_dict(self.config_dict)
            return async_client.ApiClient()

        if kubeconfig is not None:
            async with aiofiles.tempfile.NamedTemporaryFile() as temp_config:
                self.log.debug(
                    "Reading kubernetes configuration file from connection "
                    "object and writing temporary config file with its content",
                )
                await temp_config.write(kubeconfig.encode())
                await temp_config.flush()
                self._is_in_cluster = False
                await async_config.load_kube_config(
                    config_file=temp_config.name,
                    client_configuration=self.client_configuration,
                    context=cluster_context,
                )
            return async_client.ApiClient()
        self.log.debug(LOADING_KUBE_CONFIG_FILE_RESOURCE.format("default configuration file"))
        await async_config.load_kube_config(
            client_configuration=self.client_configuration,
            context=cluster_context,
        )

    async def get_conn_extras(self) -> dict:
        if self._extras is None:
            if self.conn_id:
                connection = await sync_to_async(self.get_connection)(self.conn_id)
                self._extras = connection.extra_dejson
            else:
                self._extras = {}
        return self._extras

    async def _get_field(self, field_name):
        if field_name.startswith("extra__"):
            raise ValueError(
                f"Got prefixed name {field_name}; please remove the 'extra__kubernetes__' prefix "
                "when using this method."
            )
        extras = await self.get_conn_extras()
        if field_name in extras:
            return extras.get(field_name)
        prefixed_name = f"extra__kubernetes__{field_name}"
        return extras.get(prefixed_name)

    @contextlib.asynccontextmanager
    async def get_conn(self) -> async_client.ApiClient:
        kube_client = None
        try:
            kube_client = await self._load_config() or async_client.ApiClient()
            yield kube_client
        finally:
            if kube_client is not None:
                await kube_client.close()

    async def get_pod(self, name: str, namespace: str) -> V1Pod:
        """
        Get pod's object.

        :param name: Name of the pod.
        :param namespace: Name of the pod's namespace.
        """
        async with self.get_conn() as connection:
            v1_api = async_client.CoreV1Api(connection)
            pod: V1Pod = await v1_api.read_namespaced_pod(
                name=name,
                namespace=namespace,
            )
        return pod

    async def delete_pod(self, name: str, namespace: str):
        """
        Delete pod's object.

        :param name: Name of the pod.
        :param namespace: Name of the pod's namespace.
        """
        async with self.get_conn() as connection:
            try:
                v1_api = async_client.CoreV1Api(connection)
                await v1_api.delete_namespaced_pod(
                    name=name, namespace=namespace, body=client.V1DeleteOptions()
                )
            except async_client.ApiException as e:
                # If the pod is already deleted
                if str(e.status) != "404":
                    raise

    async def read_logs(self, name: str, namespace: str):
        """
        Read logs inside the pod while starting containers inside.

        All the logs will be outputted with its timestamp to track
        the logs after the execution of the pod is completed. The
        method is used for async output of the logs only in the pod
        failed it execution or the task was cancelled by the user.

        :param name: Name of the pod.
        :param namespace: Name of the pod's namespace.
        """
        async with self.get_conn() as connection:
            try:
                v1_api = async_client.CoreV1Api(connection)
                logs = await v1_api.read_namespaced_pod_log(
                    name=name,
                    namespace=namespace,
                    follow=False,
                    timestamps=True,
                )
                logs = logs.splitlines()
                for line in logs:
                    self.log.info("Container logs from %s", line)
                return logs
            except HTTPError:
                self.log.exception("There was an error reading the kubernetes API.")
                raise

    async def get_job_status(self, name: str, namespace: str) -> V1Job:
        """
        Get job's status object.

        :param name: Name of the pod.
        :param namespace: Name of the pod's namespace.
        """
        async with self.get_conn() as connection:
            v1_api = async_client.BatchV1Api(connection)
            job: V1Job = await v1_api.read_namespaced_job_status(
                name=name,
                namespace=namespace,
            )
        return job

    async def wait_until_job_complete(self, name: str, namespace: str, poll_interval: float = 10) -> V1Job:
        """
        Block job of specified name and namespace until it is complete or failed.

        :param name: Name of Job to fetch.
        :param namespace: Namespace of the Job.
        :param poll_interval: Interval in seconds between polling the job status
        :return: Job object
        """
        while True:
            self.log.info("Requesting status for the job '%s' ", name)
            job: V1Job = await self.get_job_status(name=name, namespace=namespace)
            if self.is_job_complete(job=job):
                return job
            self.log.info("The job '%s' is incomplete. Sleeping for %i sec.", name, poll_interval)
            await asyncio.sleep(poll_interval)

    async def wait_until_container_complete(
        self, name: str, namespace: str, container_name: str, poll_interval: float = 10
    ) -> None:
        """
        Wait for the given container in the given pod to be completed.

        :param name: Name of Pod to fetch.
        :param namespace: Namespace of the Pod.
        :param container_name: name of the container within the pod to monitor
        :param poll_interval: Interval in seconds between polling the container status
        """
        while True:
            pod = await self.get_pod(name=name, namespace=namespace)
            if container_is_completed(pod=pod, container_name=container_name):
                break
            self.log.info("Waiting for container '%s' state to be completed", container_name)
            await asyncio.sleep(poll_interval)

    async def wait_until_container_started(
        self, name: str, namespace: str, container_name: str, poll_interval: float = 10
    ) -> None:
        """
        Wait for the given container in the given pod to be started.

        :param name: Name of Pod to fetch.
        :param namespace: Namespace of the Pod.
        :param container_name: name of the container within the pod to monitor
        :param poll_interval: Interval in seconds between polling the container status
        """
        while True:
            pod = await self.get_pod(name=name, namespace=namespace)
            if container_is_running(pod=pod, container_name=container_name):
                break
            self.log.info("Waiting for container '%s' state to be running", container_name)
            await asyncio.sleep(poll_interval)
