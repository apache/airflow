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
"""This module contains Google Kubernetes Engine operators."""

from __future__ import annotations

import warnings
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from google.api_core.exceptions import AlreadyExists
from kubernetes.client import V1JobList, models as k8s
from packaging.version import parse as parse_version

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.operators.kueue import (
    KubernetesInstallKueueOperator,
    KubernetesStartKueueJobOperator,
)
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.resource import (
    KubernetesCreateResourceOperator,
    KubernetesDeleteResourceOperator,
)
from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction
from airflow.providers.google.cloud.hooks.kubernetes_engine import (
    GKEHook,
    GKEKubernetesHook,
)
from airflow.providers.google.cloud.links.kubernetes_engine import (
    KubernetesEngineClusterLink,
    KubernetesEngineJobLink,
    KubernetesEnginePodLink,
    KubernetesEngineWorkloadsLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.kubernetes_engine import (
    GKEJobTrigger,
    GKEOperationTrigger,
    GKEStartPodTrigger,
)
from airflow.providers.google.common.deprecated import deprecated
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.providers_manager import ProvidersManager
from airflow.utils.timezone import utcnow

try:
    from airflow.providers.cncf.kubernetes.operators.job import KubernetesDeleteJobOperator
except ImportError:
    from airflow.exceptions import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(
        "Failed to import KubernetesDeleteJobOperator. This operator is only available in cncf-kubernetes "
        "provider version >=8.1.0"
    )

if TYPE_CHECKING:
    from google.cloud.container_v1.types import Cluster
    from kubernetes.client.models import V1Job
    from pendulum import DateTime

    from airflow.utils.context import Context

KUBE_CONFIG_ENV_VAR = "KUBECONFIG"


class GKEClusterAuthDetails:
    """
    Helper for fetching information about cluster for connecting.

    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param project_id: The Google Developers Console project id.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param cluster_hook: airflow hook for working with kubernetes cluster.
    """

    def __init__(
        self,
        cluster_name: str,
        project_id: str,
        use_internal_ip: bool,
        cluster_hook: GKEHook,
    ):
        self.cluster_name = cluster_name
        self.project_id = project_id
        self.use_internal_ip = use_internal_ip
        self.cluster_hook = cluster_hook
        self._cluster_url: str
        self._ssl_ca_cert: str

    def fetch_cluster_info(self) -> tuple[str, str]:
        """Fetch cluster info for connecting to it."""
        cluster = self.cluster_hook.get_cluster(
            name=self.cluster_name,
            project_id=self.project_id,
        )

        if not self.use_internal_ip:
            self._cluster_url = f"https://{cluster.endpoint}"
        else:
            self._cluster_url = f"https://{cluster.private_cluster_config.private_endpoint}"
        self._ssl_ca_cert = cluster.master_auth.cluster_ca_certificate
        return self._cluster_url, self._ssl_ca_cert


class GKEOperatorMixin:
    """Mixin for GKE operators provides proper unified hooks creation."""

    enable_tcp_keepalive = False

    template_fields: Sequence[str] = (
        "location",
        "cluster_name",
        "use_internal_ip",
        "project_id",
        "gcp_conn_id",
        "impersonation_chain",
    )

    @cached_property
    def cluster_hook(self) -> GKEHook:
        return GKEHook(
            gcp_conn_id=self.gcp_conn_id,  # type: ignore[attr-defined]
            location=self.location,  # type: ignore[attr-defined]
            impersonation_chain=self.impersonation_chain,  # type: ignore[attr-defined]
        )

    @cached_property
    def hook(self) -> GKEKubernetesHook:
        return GKEKubernetesHook(
            gcp_conn_id=self.gcp_conn_id,  # type: ignore[attr-defined]
            impersonation_chain=self.impersonation_chain,  # type: ignore[attr-defined]
            cluster_url=self.cluster_url,
            ssl_ca_cert=self.ssl_ca_cert,
            enable_tcp_keepalive=self.enable_tcp_keepalive,
        )

    @cached_property
    def cluster_info(self) -> tuple[str, str]:
        """Fetch cluster info for connecting to it."""
        auth_details = GKEClusterAuthDetails(
            cluster_name=self.cluster_name,  # type: ignore[attr-defined]
            project_id=self.project_id,  # type: ignore[attr-defined]
            use_internal_ip=self.use_internal_ip,  # type: ignore[attr-defined]
            cluster_hook=self.cluster_hook,
        )
        return auth_details.fetch_cluster_info()

    @property
    def cluster_url(self) -> str:
        return self.cluster_info[0]

    @property
    def ssl_ca_cert(self) -> str:
        return self.cluster_info[1]


class GKEDeleteClusterOperator(GKEOperatorMixin, GoogleCloudBaseOperator):
    """
    Deletes the cluster, including the Kubernetes endpoint and all worker nodes.

    To delete a certain cluster, you must specify the ``project_id``, the ``cluster_name``
    of the cluster, the ``location`` that the cluster is in, and the ``task_id``.

    **Operator Creation**: ::

        operator = GKEClusterDeleteOperator(
                    task_id='cluster_delete',
                    project_id='my-project',
                    location='cluster-location'
                    cluster_name='cluster-name')

    .. seealso::
        For more detail about deleting clusters have a look at the reference:
        https://google-cloud-python.readthedocs.io/en/latest/container/gapic/v1/api.html#google.cloud.container_v1.ClusterManagerClient.delete_cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKEDeleteClusterOperator`

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The Google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param name: (Deprecated) The name of the resource to delete, in this case cluster name
    :param api_version: The api version to use
    :param deferrable: Run operator in the deferrable mode.
    :param poll_interval: Interval size which defines how often operation status is checked.
    """

    template_fields: Sequence[str] = tuple(
        {"api_version", "deferrable", "poll_interval"} | set(GKEOperatorMixin.template_fields)
    )

    def __init__(
        self,
        location: str,
        use_internal_ip: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        cluster_name: str | None = None,
        name: str | None = None,
        api_version: str = "v2",
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: int = 10,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.location = location
        self.cluster_name = cluster_name or name
        self.use_internal_ip = use_internal_ip
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self._name = name
        self.api_version = api_version
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self._check_input()

    @property
    @deprecated(
        planned_removal_date="May 01, 2025",
        use_instead="cluster_name",
        category=AirflowProviderDeprecationWarning,
    )
    def name(self) -> str | None:
        return self._name

    @name.setter
    @deprecated(
        planned_removal_date="May 01, 2025",
        use_instead="cluster_name",
        category=AirflowProviderDeprecationWarning,
    )
    def name(self, name: str) -> None:
        self._name = name

    def _check_input(self) -> None:
        if not all([self.project_id, self.cluster_name, self.location]):
            self.log.error("One of (project_id, cluster_name, location) is missing or incorrect")
            raise AirflowException("Operator has incorrect or missing input.")

    def execute(self, context: Context) -> str | None:
        wait_to_complete = not self.deferrable
        operation = self.cluster_hook.delete_cluster(
            name=self.cluster_name,
            project_id=self.project_id,
            wait_to_complete=wait_to_complete,
        )

        if self.deferrable and operation is not None:
            self.defer(
                trigger=GKEOperationTrigger(
                    operation_name=operation.name,
                    project_id=self.project_id,
                    location=self.location,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )

        return operation.self_link if operation is not None else None

    def execute_complete(self, context: Context, event: dict) -> str:
        """Execute after trigger job is done."""
        status = event["status"]
        message = event["message"]

        if status in ("failed", "error"):
            self.log.exception("Trigger ended with one of the failed statuses.")
            raise AirflowException(message)

        self.log.info(message)
        operation = self.cluster_hook.get_operation(
            operation_name=event["operation_name"],
        )
        return operation.self_link


class GKECreateClusterOperator(GKEOperatorMixin, GoogleCloudBaseOperator):
    """
    Create a Google Kubernetes Engine Cluster of specified dimensions and wait until the cluster is created.

    The **minimum** required to define a cluster to create is:

    ``dict()`` ::
        cluster_def = {"name": "my-cluster-name", "initial_node_count": 1}

    or

    ``Cluster`` proto ::
        from google.cloud.container_v1.types import Cluster

        cluster_def = Cluster(name="my-cluster-name", initial_node_count=1)

    **Operator Creation**: ::

        operator = GKEClusterCreateOperator(
                    task_id='cluster_create',
                    project_id='my-project',
                    location='my-location'
                    body=cluster_def)

    .. seealso::
        For more detail on about creating clusters have a look at the reference:
        :class:`google.cloud.container_v1.types.Cluster`

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKECreateClusterOperator`

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The Google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param body: The Cluster definition to create, can be protobuf or python dict, if
        dict it must match protobuf message Cluster
    :param api_version: The api version to use
    :param deferrable: Run operator in the deferrable mode.
    :param poll_interval: Interval size which defines how often operation status is checked.
    """

    template_fields: Sequence[str] = tuple(
        {"body", "api_version", "deferrable", "poll_interval"} | set(GKEOperatorMixin.template_fields)
    )
    operator_extra_links = (KubernetesEngineClusterLink(),)

    def __init__(
        self,
        body: dict | Cluster,
        location: str,
        use_internal_ip: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        api_version: str = "v2",
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: int = 10,
        *args,
        **kwargs,
    ) -> None:
        self.body = body
        self.location = location
        self.use_internal_ip = use_internal_ip
        self.cluster_name = body.get("name") if isinstance(body, dict) else getattr(body, "name", None)
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.api_version = api_version
        self.poll_interval = poll_interval
        self.deferrable = deferrable
        self._validate_input()
        super().__init__(*args, **kwargs)

    def _validate_input(self) -> None:
        """Primary validation of the input body."""
        self._alert_deprecated_body_fields()

        error_messages: list[str] = []
        if not self._body_field("name"):
            error_messages.append("Field body['name'] is missing or incorrect")

        if self._body_field("initial_node_count"):
            if self._body_field("node_pools"):
                error_messages.append(
                    "Do not use filed body['initial_node_count'] and body['node_pools'] at the same time."
                )

        if self._body_field("node_config"):
            if self._body_field("node_pools"):
                error_messages.append(
                    "Do not use filed body['node_config'] and body['node_pools'] at the same time."
                )

        if self._body_field("node_pools"):
            if any([self._body_field("node_config"), self._body_field("initial_node_count")]):
                error_messages.append(
                    "The field body['node_pools'] should not be set if "
                    "body['node_config'] or body['initial_code_count'] are specified."
                )

        if not any([self._body_field("node_config"), self._body_field("initial_node_count")]):
            if not self._body_field("node_pools"):
                error_messages.append(
                    "Field body['node_pools'] is required if none of fields "
                    "body['initial_node_count'] or body['node_pools'] are specified."
                )

        for message in error_messages:
            self.log.error(message)

        if error_messages:
            raise AirflowException("Operator has incorrect or missing input.")

    def _body_field(self, field_name: str, default_value: Any = None) -> Any:
        """Extract the value of the given field name."""
        if isinstance(self.body, dict):
            return self.body.get(field_name, default_value)
        else:
            return getattr(self.body, field_name, default_value)

    def _alert_deprecated_body_fields(self) -> None:
        """Generate warning messages if deprecated fields were used in the body."""
        deprecated_body_fields_with_replacement = [
            ("initial_node_count", "node_pool.initial_node_count"),
            ("node_config", "node_pool.config"),
            ("zone", "location"),
            ("instance_group_urls", "node_pools.instance_group_urls"),
        ]
        for deprecated_field, replacement in deprecated_body_fields_with_replacement:
            if self._body_field(deprecated_field):
                warnings.warn(
                    f"The body field '{deprecated_field}' is deprecated. Use '{replacement}' instead.",
                    AirflowProviderDeprecationWarning,
                    stacklevel=2,
                )

    def execute(self, context: Context) -> str:
        KubernetesEngineClusterLink.persist(context=context, task_instance=self, cluster=self.body)

        try:
            operation = self.cluster_hook.create_cluster(
                cluster=self.body,
                project_id=self.project_id,
                wait_to_complete=not self.deferrable,
            )
        except AlreadyExists as error:
            self.log.info("Assuming Success: %s", error.message)
            return self.cluster_hook.get_cluster(name=self.cluster_name, project_id=self.project_id).self_link

        if self.deferrable:
            self.defer(
                trigger=GKEOperationTrigger(
                    operation_name=operation.name,
                    project_id=self.project_id,
                    location=self.location,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )

        return operation.target_link

    def execute_complete(self, context: Context, event: dict) -> str:
        status = event["status"]
        message = event["message"]

        if status in ("failed", "error"):
            self.log.exception("Trigger ended with one of the failed statuses.")
            raise AirflowException(message)

        self.log.info(message)
        operation = self.cluster_hook.get_operation(
            operation_name=event["operation_name"],
        )
        return operation.target_link


class GKEStartKueueInsideClusterOperator(GKEOperatorMixin, KubernetesInstallKueueOperator):
    """
    Installs Kueue of specific version inside Cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKEStartKueueInsideClusterOperator`

    .. seealso::
        For more details about Kueue have a look at the reference:
        https://kueue.sigs.k8s.io/docs/overview/

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The Google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    enable_tcp_keepalive = True
    template_fields = tuple(
        set(GKEOperatorMixin.template_fields) | set(KubernetesInstallKueueOperator.template_fields)
    )
    operator_extra_links = (KubernetesEngineClusterLink(),)

    def __init__(
        self,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id
        self.use_internal_ip = use_internal_ip
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        cluster = self.cluster_hook.get_cluster(name=self.cluster_name, project_id=self.project_id)
        KubernetesEngineClusterLink.persist(context=context, task_instance=self, cluster=cluster)

        if self.cluster_hook.check_cluster_autoscaling_ability(cluster=cluster):
            super().execute(context)
        else:
            self.log.info(
                "Cluster doesn't have ability to autoscale, will not install Kueue inside. Aborting"
            )


class GKEStartPodOperator(GKEOperatorMixin, KubernetesPodOperator):
    """
    Executes a task in a Kubernetes pod in the specified Google Kubernetes Engine cluster.

    This Operator assumes that the system has gcloud installed and has configured a
    connection id with a service account.

    The **minimum** required to define a cluster to create are the variables
    ``task_id``, ``project_id``, ``location``, ``cluster_name``, ``name``,
    ``namespace``, and ``image``

    .. seealso::
        For more detail about Kubernetes Engine authentication have a look at the reference:
        https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#internal_ip

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKEStartPodOperator`

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The Google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param regional: (Deprecated) The location param is region name.
    :param on_finish_action: What to do when the pod reaches its final state, or the execution is interrupted.
        If "delete_pod", the pod will be deleted regardless its state; if "delete_succeeded_pod",
        only succeeded pod will be deleted. You can set to "keep_pod" to keep the pod.
        Current default is `keep_pod`, but this will be changed in the next major release of this provider.
    :param is_delete_operator_pod: (Deprecated) What to do when the pod reaches its final
        state, or the execution is interrupted. If True, delete the
        pod; if False, leave the pod. Current default is False, but this will be
        changed in the next major release of this provider.
        Deprecated - use `on_finish_action` instead.
    :param deferrable: Run operator in the deferrable mode.
    """

    template_fields: Sequence[str] = tuple(
        {"on_finish_action", "deferrable"}
        | (set(KubernetesPodOperator.template_fields) - {"is_delete_operator_pod", "regional"})
        | set(GKEOperatorMixin.template_fields)
    )
    operator_extra_links = (KubernetesEnginePodLink(),)

    def __init__(
        self,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        regional: bool | None = None,
        on_finish_action: str | None = None,
        is_delete_operator_pod: bool | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        *args,
        **kwargs,
    ) -> None:
        if is_delete_operator_pod is not None:
            kwargs["on_finish_action"] = (
                OnFinishAction.DELETE_POD if is_delete_operator_pod else OnFinishAction.KEEP_POD
            )
        elif on_finish_action is not None:
            kwargs["on_finish_action"] = OnFinishAction(on_finish_action)
        else:
            warnings.warn(
                f"You have not set parameter `on_finish_action` in class {self.__class__.__name__}. "
                "Currently the default for this parameter is `keep_pod` but in a future release"
                " the default will be changed to `delete_pod`. To ensure pods are not deleted in"
                " the future you will need to set `on_finish_action=keep_pod` explicitly.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            kwargs["on_finish_action"] = OnFinishAction.KEEP_POD

        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id
        self.use_internal_ip = use_internal_ip
        self.impersonation_chain = impersonation_chain
        self._regional = regional
        if is_delete_operator_pod is not None:
            self.is_delete_operator_pod = is_delete_operator_pod
        self.deferrable = deferrable

        # There is no need to manage the kube_config file, as it will be generated automatically.
        # All Kubernetes parameters (except config_file) are also valid for the GKEStartPodOperator.
        if self.config_file:
            raise AirflowException("config_file is not an allowed parameter for the GKEStartPodOperator.")

    @property
    @deprecated(
        planned_removal_date="May 01, 2025",
        use_instead="on_finish_action",
        category=AirflowProviderDeprecationWarning,
    )
    def is_delete_operator_pod(self) -> bool | None:
        return self._is_delete_operator_pod

    @is_delete_operator_pod.setter
    @deprecated(
        planned_removal_date="May 01, 2025",
        use_instead="on_finish_action",
        category=AirflowProviderDeprecationWarning,
    )
    def is_delete_operator_pod(self, is_delete_operator_pod) -> None:
        self._is_delete_operator_pod = is_delete_operator_pod

    @property
    @deprecated(
        planned_removal_date="May 01, 2025",
        reason="The parameter is not in actual use.",
        category=AirflowProviderDeprecationWarning,
    )
    def regional(self) -> bool | None:
        return self._regional

    @regional.setter
    @deprecated(
        planned_removal_date="May 01, 2025",
        reason="The parameter is not in actual use.",
        category=AirflowProviderDeprecationWarning,
    )
    def regional(self, regional) -> None:
        self._regional = regional

    def invoke_defer_method(self, last_log_time: DateTime | None = None):
        """Redefine triggers which are being used in child classes."""
        trigger_start_time = utcnow()
        on_finish_action = self.on_finish_action
        if type(on_finish_action) is str and self.on_finish_action not in [i.value for i in OnFinishAction]:
            on_finish_action = self.on_finish_action.split(".")[-1].lower()  # type: ignore[assignment]
        self.defer(
            trigger=GKEStartPodTrigger(
                pod_name=self.pod.metadata.name,  # type: ignore[union-attr]
                pod_namespace=self.pod.metadata.namespace,  # type: ignore[union-attr]
                trigger_start_time=trigger_start_time,
                cluster_url=self.cluster_url,
                ssl_ca_cert=self.ssl_ca_cert,
                get_logs=self.get_logs,
                startup_timeout=self.startup_timeout_seconds,
                cluster_context=self.cluster_context,
                poll_interval=self.poll_interval,
                in_cluster=self.in_cluster,
                base_container_name=self.base_container_name,
                on_finish_action=on_finish_action,
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                logging_interval=self.logging_interval,
                last_log_time=last_log_time,
            ),
            method_name="trigger_reentry",
        )


class GKEStartJobOperator(GKEOperatorMixin, KubernetesJobOperator):
    """
    Executes a Kubernetes job in the specified Google Kubernetes Engine cluster.

    This Operator assumes that the system has gcloud installed and has configured a
    connection id with a service account.

    The **minimum** required to define a cluster to create are the variables
    ``task_id``, ``project_id``, ``location``, ``cluster_name``, ``name``,
    ``namespace``, and ``image``

    .. seealso::
        For more detail about Kubernetes Engine authentication have a look at the reference:
        https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#internal_ip

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKEStartJobOperator`

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The Google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run operator in the deferrable mode.
    :param poll_interval: (Deferrable mode only) polling period in seconds to
        check for the status of job.
    """

    template_fields: Sequence[str] = tuple(
        {"deferrable", "poll_interval"}
        | set(GKEOperatorMixin.template_fields)
        | set(KubernetesJobOperator.template_fields)
    )
    operator_extra_links = (KubernetesEngineJobLink(),)

    def __init__(
        self,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        job_poll_interval: float = 10.0,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.deferrable = deferrable
        self.job_poll_interval = job_poll_interval
        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id
        self.use_internal_ip = use_internal_ip
        self.impersonation_chain = impersonation_chain

        # There is no need to manage the kube_config file, as it will be generated automatically.
        # All Kubernetes parameters (except config_file) are also valid for the GKEStartJobOperator.
        if self.config_file:
            raise AirflowException("config_file is not an allowed parameter for the GKEStartJobOperator.")

    def execute(self, context: Context):
        """Execute process of creating Job."""
        if self.deferrable:
            kubernetes_provider = ProvidersManager().providers["apache-airflow-providers-cncf-kubernetes"]
            kubernetes_provider_name = kubernetes_provider.data["package-name"]
            kubernetes_provider_version = kubernetes_provider.version
            min_version = "8.0.1"
            if parse_version(kubernetes_provider_version) <= parse_version(min_version):
                raise AirflowException(
                    "You are trying to use `GKEStartJobOperator` in deferrable mode with the provider "
                    f"package {kubernetes_provider_name}=={kubernetes_provider_version} which doesn't "
                    f"support this feature. Please upgrade it to version higher than {min_version}."
                )
        return super().execute(context)

    def execute_deferrable(self):
        self.defer(
            trigger=GKEJobTrigger(
                cluster_url=self.cluster_url,
                ssl_ca_cert=self.ssl_ca_cert,
                job_name=self.job.metadata.name,  # type: ignore[union-attr]
                job_namespace=self.job.metadata.namespace,  # type: ignore[union-attr]
                pod_name=self.pod.metadata.name,  # type: ignore[union-attr]
                pod_namespace=self.pod.metadata.namespace,  # type: ignore[union-attr]
                base_container_name=self.base_container_name,
                gcp_conn_id=self.gcp_conn_id,
                poll_interval=self.job_poll_interval,
                impersonation_chain=self.impersonation_chain,
                get_logs=self.get_logs,
                do_xcom_push=self.do_xcom_push,
            ),
            method_name="execute_complete",
        )


class GKEDescribeJobOperator(GKEOperatorMixin, GoogleCloudBaseOperator):
    """
    Retrieve information about Job by given name.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKEDescribeJobOperator`

    :param job_name: The name of the Job to delete
    :param namespace: The name of the Google Kubernetes Engine namespace.
    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The Google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple({"job_name", "namespace"} | set(GKEOperatorMixin.template_fields))
    operator_extra_links = (KubernetesEngineJobLink(),)

    def __init__(
        self,
        job_name: str,
        namespace: str,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.job_name = job_name
        self.namespace = namespace
        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id
        self.use_internal_ip = use_internal_ip
        self.impersonation_chain = impersonation_chain
        self.job: V1Job | None = None

    def execute(self, context: Context) -> None:
        self.job = self.hook.get_job(job_name=self.job_name, namespace=self.namespace)
        self.log.info(
            "Retrieved description of Job %s from cluster %s:\n %s",
            self.job_name,
            self.cluster_name,
            self.job,
        )
        KubernetesEngineJobLink.persist(context=context, task_instance=self)
        return None


class GKEListJobsOperator(GKEOperatorMixin, GoogleCloudBaseOperator):
    """
    Retrieve list of Jobs.

    If namespace parameter is specified, the list of Jobs from dedicated
    namespace will be retrieved. If no namespace specified, it will output Jobs from all namespaces.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKEListJobsOperator`

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The Google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param namespace: The name of the Google Kubernetes Engine namespace.
    :param do_xcom_push: If set to True the result list of Jobs will be pushed to the task result.
    """

    template_fields: Sequence[str] = tuple({"namespace"} | set(GKEOperatorMixin.template_fields))
    operator_extra_links = (KubernetesEngineWorkloadsLink(),)

    def __init__(
        self,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        namespace: str | None = None,
        do_xcom_push: bool = True,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id
        self.use_internal_ip = use_internal_ip
        self.impersonation_chain = impersonation_chain
        self.namespace = namespace
        self.do_xcom_push = do_xcom_push

    def execute(self, context: Context) -> dict:
        if self.namespace:
            jobs = self.hook.list_jobs_from_namespace(namespace=self.namespace)
        else:
            jobs = self.hook.list_jobs_all_namespaces()
        for job in jobs.items:
            self.log.info("Retrieved description of Job:\n %s", job)
        if self.do_xcom_push:
            ti = context["ti"]
            ti.xcom_push(key="jobs_list", value=V1JobList.to_dict(jobs))
        KubernetesEngineWorkloadsLink.persist(context=context, task_instance=self)
        return V1JobList.to_dict(jobs)


class GKECreateCustomResourceOperator(GKEOperatorMixin, KubernetesCreateResourceOperator):
    """
    Create a resource in the specified Google Kubernetes Engine cluster.

    This Operator assumes that the system has gcloud installed and has configured a
    connection id with a service account.

    .. seealso::
        For more detail about Kubernetes Engine authentication have a look at the reference:
        https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#internal_ip

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKECreateCustomResourceOperator`

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The Google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        set(GKEOperatorMixin.template_fields) | set(KubernetesCreateResourceOperator.template_fields)
    )

    def __init__(
        self,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id
        self.use_internal_ip = use_internal_ip
        self.impersonation_chain = impersonation_chain

        if self.gcp_conn_id is None:
            raise AirflowException(
                "The gcp_conn_id parameter has become required. If you want to use Application Default "
                "Credentials (ADC) strategy for authorization, create an empty connection "
                "called `google_cloud_default`.",
            )
        # There is no need to manage the kube_config file, as it will be generated automatically.
        # All Kubernetes parameters (except config_file) are also valid for the GKECreateCustomResourceOperator.
        if self.config_file:
            raise AirflowException(
                "config_file is not an allowed parameter for the GKECreateCustomResourceOperator."
            )


class GKEDeleteCustomResourceOperator(GKEOperatorMixin, KubernetesDeleteResourceOperator):
    """
    Delete a resource in the specified Google Kubernetes Engine cluster.

    This Operator assumes that the system has gcloud installed and has configured a
    connection id with a service account.

    .. seealso::
        For more detail about Kubernetes Engine authentication have a look at the reference:
        https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#internal_ip

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKEDeleteCustomResourceOperator`

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The Google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        set(GKEOperatorMixin.template_fields) | set(KubernetesDeleteResourceOperator.template_fields)
    )

    def __init__(
        self,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id
        self.use_internal_ip = use_internal_ip
        self.impersonation_chain = impersonation_chain

        if self.gcp_conn_id is None:
            raise AirflowException(
                "The gcp_conn_id parameter has become required. If you want to use Application Default "
                "Credentials (ADC) strategy for authorization, create an empty connection "
                "called `google_cloud_default`.",
            )
        # There is no need to manage the kube_config file, as it will be generated automatically.
        # All Kubernetes parameters (except config_file) are also valid for the GKEDeleteCustomResourceOperator.
        if self.config_file:
            raise AirflowException(
                "config_file is not an allowed parameter for the GKEDeleteCustomResourceOperator."
            )


class GKEStartKueueJobOperator(GKEOperatorMixin, KubernetesStartKueueJobOperator):
    """
    Executes a Kubernetes Job in Kueue in the specified Google Kubernetes Engine cluster.

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The Google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = tuple(
        set(GKEOperatorMixin.template_fields) | set(KubernetesStartKueueJobOperator.template_fields)
    )

    def __init__(
        self,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id
        self.use_internal_ip = use_internal_ip
        self.impersonation_chain = impersonation_chain


class GKEDeleteJobOperator(GKEOperatorMixin, KubernetesDeleteJobOperator):
    """
    Delete a Kubernetes job in the specified Google Kubernetes Engine cluster.

    This Operator assumes that the system has gcloud installed and has configured a
    connection id with a service account.

    The **minimum** required to define a cluster to create are the variables
    ``task_id``, ``project_id``, ``location``, ``cluster_name``, ``name``,
    ``namespace``

    .. seealso::
        For more detail about Kubernetes Engine authentication have a look at the reference:
        https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#internal_ip

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKEDeleteJobOperator`

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The Google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        set(GKEOperatorMixin.template_fields) | set(KubernetesDeleteJobOperator.template_fields)
    )

    def __init__(
        self,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id
        self.use_internal_ip = use_internal_ip
        self.impersonation_chain = impersonation_chain

        if self.gcp_conn_id is None:
            raise AirflowException(
                "The gcp_conn_id parameter has become required. If you want to use Application Default "
                "Credentials (ADC) strategy for authorization, create an empty connection "
                "called `google_cloud_default`.",
            )
        # There is no need to manage the kube_config file, as it will be generated automatically.
        # All Kubernetes parameters (except config_file) are also valid for the GKEDeleteJobOperator.
        if self.config_file:
            raise AirflowException("config_file is not an allowed parameter for the GKEDeleteJobOperator.")


class GKESuspendJobOperator(GKEOperatorMixin, GoogleCloudBaseOperator):
    """
    Suspend Job by given name.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKESuspendJobOperator`

    :param name: The name of the Job to suspend
    :param namespace: The name of the Google Kubernetes Engine namespace.
    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The Google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple({"name", "namespace"} | set(GKEOperatorMixin.template_fields))
    operator_extra_links = (KubernetesEngineJobLink(),)

    def __init__(
        self,
        name: str,
        namespace: str,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.name = name
        self.namespace = namespace
        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id
        self.use_internal_ip = use_internal_ip
        self.impersonation_chain = impersonation_chain
        self.job: V1Job | None = None

    def execute(self, context: Context) -> None:
        self.job = self.hook.patch_namespaced_job(
            job_name=self.name,
            namespace=self.namespace,
            body={"spec": {"suspend": True}},
        )
        self.log.info(
            "Job %s from cluster %s was suspended.",
            self.name,
            self.cluster_name,
        )
        KubernetesEngineJobLink.persist(context=context, task_instance=self)

        return k8s.V1Job.to_dict(self.job)


class GKEResumeJobOperator(GKEOperatorMixin, GoogleCloudBaseOperator):
    """
    Resume Job by given name.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKEResumeJobOperator`

    :param name: The name of the Job to resume
    :param namespace: The name of the Google Kubernetes Engine namespace.
    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster.
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The Google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple({"name", "namespace"} | set(GKEOperatorMixin.template_fields))
    operator_extra_links = (KubernetesEngineJobLink(),)

    def __init__(
        self,
        name: str,
        namespace: str,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.name = name
        self.namespace = namespace
        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id
        self.use_internal_ip = use_internal_ip
        self.impersonation_chain = impersonation_chain
        self.job: V1Job | None = None

    def execute(self, context: Context) -> None:
        self.job = self.hook.patch_namespaced_job(
            job_name=self.name,
            namespace=self.namespace,
            body={"spec": {"suspend": False}},
        )
        self.log.info(
            "Job %s from cluster %s was resumed.",
            self.name,
            self.cluster_name,
        )
        KubernetesEngineJobLink.persist(context=context, task_instance=self)

        return k8s.V1Job.to_dict(self.job)
