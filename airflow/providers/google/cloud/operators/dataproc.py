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
"""This module contains Google Dataproc operators."""
from __future__ import annotations

import inspect
import ntpath
import os
import re
import time
import uuid
import warnings
from datetime import datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Sequence

from google.api_core.exceptions import AlreadyExists, NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry, exponential_sleep_generator
from google.cloud.dataproc_v1 import Batch, Cluster, ClusterStatus, JobStatus

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook, DataProcJobBuilder
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.links.dataproc import (
    DATAPROC_BATCH_LINK,
    DATAPROC_CLUSTER_LINK_DEPRECATED,
    DATAPROC_JOB_LINK_DEPRECATED,
    DataprocBatchesListLink,
    DataprocBatchLink,
    DataprocClusterLink,
    DataprocJobLink,
    DataprocLink,
    DataprocWorkflowLink,
    DataprocWorkflowTemplateLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.cloud.triggers.dataproc import (
    DataprocBatchTrigger,
    DataprocClusterTrigger,
    DataprocDeleteClusterTrigger,
    DataprocSubmitTrigger,
    DataprocWorkflowTrigger,
)
from airflow.utils import timezone

if TYPE_CHECKING:
    from google.api_core import operation
    from google.protobuf.duration_pb2 import Duration
    from google.protobuf.field_mask_pb2 import FieldMask

    from airflow.utils.context import Context


class PreemptibilityType(Enum):
    """Contains possible Type values of Preemptibility applicable for every secondary worker of Cluster."""

    PREEMPTIBLE = "PREEMPTIBLE"
    SPOT = "SPOT"
    PREEMPTIBILITY_UNSPECIFIED = "PREEMPTIBILITY_UNSPECIFIED"


class ClusterGenerator:
    """Create a new Dataproc Cluster.

    :param cluster_name: The name of the DataProc cluster to create. (templated)
    :param project_id: The ID of the google cloud project in which
        to create the cluster. (templated)
    :param num_workers: The # of workers to spin up. If set to zero will
        spin up cluster in a single node mode
    :param storage_bucket: The storage bucket to use, setting to None lets dataproc
        generate a custom one for you
    :param init_actions_uris: List of GCS uri's containing
        dataproc initialization scripts
    :param init_action_timeout: Amount of time executable scripts in
        init_actions_uris has to complete
    :param metadata: dict of key-value google compute engine metadata entries
        to add to all instances
    :param image_version: the version of software inside the Dataproc cluster
    :param custom_image: custom Dataproc image for more info see
        https://cloud.google.com/dataproc/docs/guides/dataproc-images
    :param custom_image_project_id: project id for the custom Dataproc image, for more info see
        https://cloud.google.com/dataproc/docs/guides/dataproc-images
    :param custom_image_family: family for the custom Dataproc image,
        family name can be provide using --family flag while creating custom image, for more info see
        https://cloud.google.com/dataproc/docs/guides/dataproc-images
    :param autoscaling_policy: The autoscaling policy used by the cluster. Only resource names
        including projectid and location (region) are valid. Example:
        ``projects/[projectId]/locations/[dataproc_region]/autoscalingPolicies/[policy_id]``
    :param properties: dict of properties to set on
        config files (e.g. spark-defaults.conf), see
        https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters#SoftwareConfig
    :param optional_components: List of optional cluster components, for more info see
        https://cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig#Component
    :param num_masters: The # of master nodes to spin up
    :param master_machine_type: Compute engine machine type to use for the primary node
    :param master_disk_type: Type of the boot disk for the primary node
        (default is ``pd-standard``).
        Valid values: ``pd-ssd`` (Persistent Disk Solid State Drive) or
        ``pd-standard`` (Persistent Disk Hard Disk Drive).
    :param master_disk_size: Disk size for the primary node
    :param worker_machine_type: Compute engine machine type to use for the worker nodes
    :param worker_disk_type: Type of the boot disk for the worker node
        (default is ``pd-standard``).
        Valid values: ``pd-ssd`` (Persistent Disk Solid State Drive) or
        ``pd-standard`` (Persistent Disk Hard Disk Drive).
    :param worker_disk_size: Disk size for the worker nodes
    :param num_preemptible_workers: The # of VM instances in the instance group as secondary workers
        inside the cluster with Preemptibility enabled by default.
        Note, that it is not possible to mix non-preemptible and preemptible secondary workers in
        one cluster.
    :param preemptibility: The type of Preemptibility applicable for every secondary worker, see
        https://cloud.google.com/dataproc/docs/reference/rpc/ \
        google.cloud.dataproc.v1#google.cloud.dataproc.v1.InstanceGroupConfig.Preemptibility
    :param zone: The zone where the cluster will be located. Set to None to auto-zone. (templated)
    :param network_uri: The network uri to be used for machine communication, cannot be
        specified with subnetwork_uri
    :param subnetwork_uri: The subnetwork uri to be used for machine communication,
        cannot be specified with network_uri
    :param internal_ip_only: If true, all instances in the cluster will only
        have internal IP addresses. This can only be enabled for subnetwork
        enabled networks
    :param tags: The GCE tags to add to all instances
    :param region: The specified region where the dataproc cluster is created.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param service_account: The service account of the dataproc instances.
    :param service_account_scopes: The URIs of service account scopes to be included.
    :param idle_delete_ttl: The longest duration that cluster would keep alive while
        staying idle. Passing this threshold will cause cluster to be auto-deleted.
        A duration in seconds.
    :param auto_delete_time:  The time when cluster will be auto-deleted.
    :param auto_delete_ttl: The life duration of cluster, the cluster will be
        auto-deleted at the end of this duration.
        A duration in seconds. (If auto_delete_time is set this parameter will be ignored)
    :param customer_managed_key: The customer-managed key used for disk encryption
        ``projects/[PROJECT_STORING_KEYS]/locations/[LOCATION]/keyRings/[KEY_RING_NAME]/cryptoKeys/[KEY_NAME]`` # noqa
    :param enable_component_gateway: Provides access to the web interfaces of default and selected optional
        components on the cluster.
    """  # noqa: E501

    def __init__(
        self,
        project_id: str,
        num_workers: int | None = None,
        zone: str | None = None,
        network_uri: str | None = None,
        subnetwork_uri: str | None = None,
        internal_ip_only: bool | None = None,
        tags: list[str] | None = None,
        storage_bucket: str | None = None,
        init_actions_uris: list[str] | None = None,
        init_action_timeout: str = "10m",
        metadata: dict | None = None,
        custom_image: str | None = None,
        custom_image_project_id: str | None = None,
        custom_image_family: str | None = None,
        image_version: str | None = None,
        autoscaling_policy: str | None = None,
        properties: dict | None = None,
        optional_components: list[str] | None = None,
        num_masters: int = 1,
        master_machine_type: str = "n1-standard-4",
        master_disk_type: str = "pd-standard",
        master_disk_size: int = 1024,
        worker_machine_type: str = "n1-standard-4",
        worker_disk_type: str = "pd-standard",
        worker_disk_size: int = 1024,
        num_preemptible_workers: int = 0,
        preemptibility: str = PreemptibilityType.PREEMPTIBLE.value,
        service_account: str | None = None,
        service_account_scopes: list[str] | None = None,
        idle_delete_ttl: int | None = None,
        auto_delete_time: datetime | None = None,
        auto_delete_ttl: int | None = None,
        customer_managed_key: str | None = None,
        enable_component_gateway: bool | None = False,
        **kwargs,
    ) -> None:

        self.project_id = project_id
        self.num_masters = num_masters
        self.num_workers = num_workers
        self.num_preemptible_workers = num_preemptible_workers
        self.preemptibility = self._set_preemptibility_type(preemptibility)
        self.storage_bucket = storage_bucket
        self.init_actions_uris = init_actions_uris
        self.init_action_timeout = init_action_timeout
        self.metadata = metadata
        self.custom_image = custom_image
        self.custom_image_project_id = custom_image_project_id
        self.custom_image_family = custom_image_family
        self.image_version = image_version
        self.properties = properties or {}
        self.optional_components = optional_components
        self.master_machine_type = master_machine_type
        self.master_disk_type = master_disk_type
        self.master_disk_size = master_disk_size
        self.autoscaling_policy = autoscaling_policy
        self.worker_machine_type = worker_machine_type
        self.worker_disk_type = worker_disk_type
        self.worker_disk_size = worker_disk_size
        self.zone = zone
        self.network_uri = network_uri
        self.subnetwork_uri = subnetwork_uri
        self.internal_ip_only = internal_ip_only
        self.tags = tags
        self.service_account = service_account
        self.service_account_scopes = service_account_scopes
        self.idle_delete_ttl = idle_delete_ttl
        self.auto_delete_time = auto_delete_time
        self.auto_delete_ttl = auto_delete_ttl
        self.customer_managed_key = customer_managed_key
        self.enable_component_gateway = enable_component_gateway
        self.single_node = num_workers == 0

        if self.custom_image and self.image_version:
            raise ValueError("The custom_image and image_version can't be both set")

        if self.custom_image_family and self.image_version:
            raise ValueError("The image_version and custom_image_family can't be both set")

        if self.custom_image_family and self.custom_image:
            raise ValueError("The custom_image and custom_image_family can't be both set")

        if self.single_node and self.num_preemptible_workers > 0:
            raise ValueError("Single node cannot have preemptible workers.")

    def _set_preemptibility_type(self, preemptibility: str):
        return PreemptibilityType(preemptibility.upper())

    def _get_init_action_timeout(self) -> dict:
        match = re.fullmatch(r"(\d+)([sm])", self.init_action_timeout)
        if match:
            val = int(match.group(1))
            unit = match.group(2)
            if unit == "s":
                return {"seconds": val}
            elif unit == "m":
                return {"seconds": int(timedelta(minutes=val).total_seconds())}

        raise AirflowException(
            "DataprocClusterCreateOperator init_action_timeout"
            " should be expressed in minutes or seconds. i.e. 10m, 30s"
        )

    def _build_gce_cluster_config(self, cluster_data):
        # This variable is created since same string was being used multiple times
        config = "gce_cluster_config"
        if self.zone:
            zone_uri = f"https://www.googleapis.com/compute/v1/projects/{self.project_id}/zones/{self.zone}"
            cluster_data[config]["zone_uri"] = zone_uri

        if self.metadata:
            cluster_data[config]["metadata"] = self.metadata

        if self.network_uri:
            cluster_data[config]["network_uri"] = self.network_uri

        if self.subnetwork_uri:
            cluster_data[config]["subnetwork_uri"] = self.subnetwork_uri

        if self.internal_ip_only:
            if not self.subnetwork_uri:
                raise AirflowException("Set internal_ip_only to true only when you pass a subnetwork_uri.")
            cluster_data[config]["internal_ip_only"] = True

        if self.tags:
            cluster_data[config]["tags"] = self.tags

        if self.service_account:
            cluster_data[config]["service_account"] = self.service_account

        if self.service_account_scopes:
            cluster_data[config]["service_account_scopes"] = self.service_account_scopes

        return cluster_data

    def _build_lifecycle_config(self, cluster_data):
        # This variable is created since same string was being used multiple times
        lifecycle_config = "lifecycle_config"
        if self.idle_delete_ttl:
            cluster_data[lifecycle_config]["idle_delete_ttl"] = {"seconds": self.idle_delete_ttl}

        if self.auto_delete_time:
            utc_auto_delete_time = timezone.convert_to_utc(self.auto_delete_time)
            cluster_data[lifecycle_config]["auto_delete_time"] = utc_auto_delete_time.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )
        elif self.auto_delete_ttl:
            cluster_data[lifecycle_config]["auto_delete_ttl"] = {"seconds": int(self.auto_delete_ttl)}

        return cluster_data

    def _build_cluster_data(self):
        if self.zone:
            master_type_uri = (
                f"projects/{self.project_id}/zones/{self.zone}/machineTypes/{self.master_machine_type}"
            )
            worker_type_uri = (
                f"projects/{self.project_id}/zones/{self.zone}/machineTypes/{self.worker_machine_type}"
            )
        else:
            master_type_uri = self.master_machine_type
            worker_type_uri = self.worker_machine_type

        cluster_data = {
            "gce_cluster_config": {},
            "master_config": {
                "num_instances": self.num_masters,
                "machine_type_uri": master_type_uri,
                "disk_config": {
                    "boot_disk_type": self.master_disk_type,
                    "boot_disk_size_gb": self.master_disk_size,
                },
            },
            "worker_config": {
                "num_instances": self.num_workers,
                "machine_type_uri": worker_type_uri,
                "disk_config": {
                    "boot_disk_type": self.worker_disk_type,
                    "boot_disk_size_gb": self.worker_disk_size,
                },
            },
            "secondary_worker_config": {},
            "software_config": {},
            "lifecycle_config": {},
            "encryption_config": {},
            "autoscaling_config": {},
            "endpoint_config": {},
        }
        if self.num_preemptible_workers > 0:
            cluster_data["secondary_worker_config"] = {
                "num_instances": self.num_preemptible_workers,
                "machine_type_uri": worker_type_uri,
                "disk_config": {
                    "boot_disk_type": self.worker_disk_type,
                    "boot_disk_size_gb": self.worker_disk_size,
                },
                "is_preemptible": True,
                "preemptibility": self.preemptibility.value,
            }

        if self.storage_bucket:
            cluster_data["config_bucket"] = self.storage_bucket

        if self.image_version:
            cluster_data["software_config"]["image_version"] = self.image_version

        elif self.custom_image:
            project_id = self.custom_image_project_id or self.project_id
            custom_image_url = (
                f"https://www.googleapis.com/compute/beta/projects/{project_id}"
                f"/global/images/{self.custom_image}"
            )
            cluster_data["master_config"]["image_uri"] = custom_image_url
            if not self.single_node:
                cluster_data["worker_config"]["image_uri"] = custom_image_url

        elif self.custom_image_family:
            project_id = self.custom_image_project_id or self.project_id
            custom_image_url = (
                "https://www.googleapis.com/compute/beta/projects/"
                f"{project_id}/global/images/family/{self.custom_image_family}"
            )
            cluster_data["master_config"]["image_uri"] = custom_image_url
            if not self.single_node:
                cluster_data["worker_config"]["image_uri"] = custom_image_url

        cluster_data = self._build_gce_cluster_config(cluster_data)

        if self.single_node:
            self.properties["dataproc:dataproc.allow.zero.workers"] = "true"

        if self.properties:
            cluster_data["software_config"]["properties"] = self.properties

        if self.optional_components:
            cluster_data["software_config"]["optional_components"] = self.optional_components

        cluster_data = self._build_lifecycle_config(cluster_data)

        if self.init_actions_uris:
            init_actions_dict = [
                {"executable_file": uri, "execution_timeout": self._get_init_action_timeout()}
                for uri in self.init_actions_uris
            ]
            cluster_data["initialization_actions"] = init_actions_dict

        if self.customer_managed_key:
            cluster_data["encryption_config"] = {"gce_pd_kms_key_name": self.customer_managed_key}
        if self.autoscaling_policy:
            cluster_data["autoscaling_config"] = {"policy_uri": self.autoscaling_policy}
        if self.enable_component_gateway:
            cluster_data["endpoint_config"] = {"enable_http_port_access": self.enable_component_gateway}

        return cluster_data

    def make(self):
        """
        Helper method for easier migration.

        :return: Dict representing Dataproc cluster.
        """
        return self._build_cluster_data()


class DataprocCreateClusterOperator(GoogleCloudBaseOperator):
    """Create a new cluster on Google Cloud Dataproc.

    The operator will wait until the creation is successful or an error occurs
    in the creation process.

    If the cluster already exists and ``use_if_exists`` is True, the operator will:

    If the cluster already exists and ``use_if_exists`` is True then the operator will:
    - if cluster state is ERROR then delete it if specified and raise error
    - if cluster state is CREATING wait for it and then check for ERROR state
    - if cluster state is DELETING wait for it and then create new cluster

    Please refer to
    https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters

    for a detailed explanation on the different parameters. Most of the configuration
    parameters detailed in the link are available as a parameter to this operator.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataprocCreateClusterOperator`

    :param project_id: The ID of the Google cloud project in which
        to create the cluster. (templated)
    :param cluster_name: Name of the cluster to create
    :param labels: Labels that will be assigned to created cluster. Please, notice that
        adding labels to ClusterConfig object in cluster_config parameter will not lead
        to adding labels to the cluster. Labels for the clusters could be only set by passing
        values to parameter of DataprocCreateCluster operator.
    :param cluster_config: Required. The cluster config to create.
        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.dataproc_v1.types.ClusterConfig`
    :param virtual_cluster_config: Optional. The virtual cluster config, used when creating a Dataproc
        cluster that does not directly control the underlying compute resources, for example, when creating a
        `Dataproc-on-GKE cluster
        <https://cloud.google.com/dataproc/docs/concepts/jobs/dataproc-gke#create-a-dataproc-on-gke-cluster>`
    :param region: The specified region where the dataproc cluster is created.
    :param delete_on_error: If true the cluster will be deleted if created with ERROR state. Default
        value is true.
    :param use_if_exists: If true use existing cluster
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``DeleteClusterRequest`` requests with the same id, then the second request will be ignored and the
        first ``google.longrunning.Operation`` created and stored in the backend is returned.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run operator in the deferrable mode.
    :param polling_interval_seconds: Time (seconds) to wait between calls to check the run status.
    """

    template_fields: Sequence[str] = (
        "project_id",
        "region",
        "cluster_config",
        "virtual_cluster_config",
        "cluster_name",
        "labels",
        "impersonation_chain",
    )
    template_fields_renderers = {"cluster_config": "json", "virtual_cluster_config": "json"}

    operator_extra_links = (DataprocClusterLink(),)

    def __init__(
        self,
        *,
        cluster_name: str,
        region: str,
        project_id: str | None = None,
        cluster_config: dict | Cluster | None = None,
        virtual_cluster_config: dict | None = None,
        labels: dict | None = None,
        request_id: str | None = None,
        delete_on_error: bool = True,
        use_if_exists: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float = 1 * 60 * 60,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        polling_interval_seconds: int = 10,
        **kwargs,
    ) -> None:
        # TODO: remove one day
        if cluster_config is None and virtual_cluster_config is None:
            warnings.warn(
                f"Passing cluster parameters by keywords to `{type(self).__name__}` will be deprecated. "
                "Please provide cluster_config object using `cluster_config` parameter. "
                "You can use `airflow.dataproc.ClusterGenerator.generate_cluster` "
                "method to obtain cluster object.",
                AirflowProviderDeprecationWarning,
                stacklevel=1,
            )
            # Remove result of apply defaults
            if "params" in kwargs:
                del kwargs["params"]

            # Create cluster object from kwargs
            if project_id is None:
                raise AirflowException(
                    "project_id argument is required when building cluster from keywords parameters"
                )
            kwargs["project_id"] = project_id
            cluster_config = ClusterGenerator(**kwargs).make()

            # Remove from kwargs cluster params passed for backward compatibility
            cluster_params = inspect.signature(ClusterGenerator.__init__).parameters
            for arg in cluster_params:
                if arg in kwargs:
                    del kwargs[arg]

        super().__init__(**kwargs)
        if deferrable and polling_interval_seconds <= 0:
            raise ValueError("Invalid value for polling_interval_seconds. Expected value greater than 0")
        self.cluster_config = cluster_config
        self.cluster_name = cluster_name
        self.labels = labels
        self.project_id = project_id
        self.region = region
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.delete_on_error = delete_on_error
        self.use_if_exists = use_if_exists
        self.impersonation_chain = impersonation_chain
        self.virtual_cluster_config = virtual_cluster_config
        self.deferrable = deferrable
        self.polling_interval_seconds = polling_interval_seconds

    def _create_cluster(self, hook: DataprocHook):
        return hook.create_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
            labels=self.labels,
            cluster_config=self.cluster_config,
            virtual_cluster_config=self.virtual_cluster_config,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

    def _delete_cluster(self, hook):
        self.log.info("Deleting the cluster")
        hook.delete_cluster(region=self.region, cluster_name=self.cluster_name, project_id=self.project_id)

    def _get_cluster(self, hook: DataprocHook) -> Cluster:
        return hook.get_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

    def _handle_error_state(self, hook: DataprocHook, cluster: Cluster) -> None:
        if cluster.status.state != cluster.status.State.ERROR:
            return
        self.log.info("Cluster is in ERROR state")
        self.log.info("Gathering diagnostic information.")
        gcs_uri = hook.diagnose_cluster(
            region=self.region, cluster_name=self.cluster_name, project_id=self.project_id
        )
        self.log.info("Diagnostic information for cluster %s available at: %s", self.cluster_name, gcs_uri)
        if self.delete_on_error:
            self._delete_cluster(hook)
            # The delete op is asynchronous and can cause further failure if the cluster finishes
            # deleting between catching AlreadyExists and checking state
            self._wait_for_cluster_in_deleting_state(hook)
            raise AirflowException("Cluster was created in an ERROR state then deleted.")
        raise AirflowException("Cluster was created but is in ERROR state")

    def _wait_for_cluster_in_deleting_state(self, hook: DataprocHook) -> None:
        time_left = self.timeout
        for time_to_sleep in exponential_sleep_generator(initial=10, maximum=120):
            if time_left < 0:
                raise AirflowException(f"Cluster {self.cluster_name} is still DELETING state, aborting")
            time.sleep(time_to_sleep)
            time_left -= time_to_sleep
            try:
                self._get_cluster(hook)
            except NotFound:
                break

    def _wait_for_cluster_in_creating_state(self, hook: DataprocHook) -> Cluster:
        time_left = self.timeout
        cluster = self._get_cluster(hook)
        for time_to_sleep in exponential_sleep_generator(initial=10, maximum=120):
            if cluster.status.state != cluster.status.State.CREATING:
                break
            if time_left < 0:
                raise AirflowException(f"Cluster {self.cluster_name} is still CREATING state, aborting")
            time.sleep(time_to_sleep)
            time_left -= time_to_sleep
            cluster = self._get_cluster(hook)
        return cluster

    def execute(self, context: Context) -> dict:
        self.log.info("Creating cluster: %s", self.cluster_name)
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        # Save data required to display extra link no matter what the cluster status will be
        project_id = self.project_id or hook.project_id
        if project_id:
            DataprocClusterLink.persist(
                context=context,
                operator=self,
                cluster_id=self.cluster_name,
                project_id=project_id,
                region=self.region,
            )
        try:
            # First try to create a new cluster
            operation = self._create_cluster(hook)
            if not self.deferrable:
                cluster = hook.wait_for_operation(
                    timeout=self.timeout, result_retry=self.retry, operation=operation
                )
                self.log.info("Cluster created.")
                return Cluster.to_dict(cluster)
            else:
                self.defer(
                    trigger=DataprocClusterTrigger(
                        cluster_name=self.cluster_name,
                        project_id=self.project_id,
                        region=self.region,
                        gcp_conn_id=self.gcp_conn_id,
                        impersonation_chain=self.impersonation_chain,
                        polling_interval_seconds=self.polling_interval_seconds,
                    ),
                    method_name="execute_complete",
                )
        except AlreadyExists:
            if not self.use_if_exists:
                raise
            self.log.info("Cluster already exists.")
            cluster = self._get_cluster(hook)
        except AirflowException as ae:
            # There still could be a cluster created here in an ERROR state which
            # should be deleted immediately rather than consuming another retry attempt
            # (assuming delete_on_error is true (default))
            # This reduces overall the number of task attempts from 3 to 2 to successful cluster creation
            # assuming the underlying GCE issues have resolved within that window. Users can configure
            # a higher number of retry attempts in powers of two with 30s-60s wait interval
            try:
                cluster = self._get_cluster(hook)
                self._handle_error_state(hook, cluster)
            except AirflowException as ae_inner:
                # We could get any number of failures here, including cluster not found and we
                # can just ignore to ensure we surface the original cluster create failure
                self.log.error(ae_inner, exc_info=True)
            finally:
                raise ae

        # Check if cluster is not in ERROR state
        self._handle_error_state(hook, cluster)
        if cluster.status.state == cluster.status.State.CREATING:
            # Wait for cluster to be created
            cluster = self._wait_for_cluster_in_creating_state(hook)
            self._handle_error_state(hook, cluster)
        elif cluster.status.state == cluster.status.State.DELETING:
            # Wait for cluster to be deleted
            self._wait_for_cluster_in_deleting_state(hook)
            # Create new cluster
            cluster = self._create_cluster(hook)
            self._handle_error_state(hook, cluster)

        return Cluster.to_dict(cluster)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        """
        Callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        cluster_state = event["cluster_state"]
        cluster_name = event["cluster_name"]

        if cluster_state == ClusterStatus.State.ERROR:
            raise AirflowException(f"Cluster is in ERROR state:\n{cluster_name}")

        self.log.info("%s completed successfully.", self.task_id)
        return event["cluster"]


class DataprocScaleClusterOperator(GoogleCloudBaseOperator):
    """Scale, up or down, a cluster on Google Cloud Dataproc.

    The operator will wait until the cluster is re-scaled.

    Example usage:

    .. code-block:: python

        t1 = DataprocClusterScaleOperator(
            task_id="dataproc_scale",
            project_id="my-project",
            cluster_name="cluster-1",
            num_workers=10,
            num_preemptible_workers=10,
            graceful_decommission_timeout="1h",
        )

    .. seealso::
        For more detail on about scaling clusters have a look at the reference:
        https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/scaling-clusters

    :param cluster_name: The name of the cluster to scale. (templated)
    :param project_id: The ID of the google cloud project in which
        the cluster runs. (templated)
    :param region: The region for the dataproc cluster. (templated)
    :param num_workers: The new number of workers
    :param num_preemptible_workers: The new number of preemptible workers
    :param graceful_decommission_timeout: Timeout for graceful YARN decommissioning.
        Maximum value is 1d
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = ("cluster_name", "project_id", "region", "impersonation_chain")

    operator_extra_links = (DataprocLink(),)

    def __init__(
        self,
        *,
        cluster_name: str,
        project_id: str | None = None,
        region: str = "global",
        num_workers: int = 2,
        num_preemptible_workers: int = 0,
        graceful_decommission_timeout: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.num_workers = num_workers
        self.num_preemptible_workers = num_preemptible_workers
        self.graceful_decommission_timeout = graceful_decommission_timeout
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

        # TODO: Remove one day
        warnings.warn(
            f"The `{type(self).__name__}` operator is deprecated, "
            "please use `DataprocUpdateClusterOperator` instead.",
            AirflowProviderDeprecationWarning,
            stacklevel=1,
        )

    def _build_scale_cluster_data(self) -> dict:
        scale_data = {
            "config": {
                "worker_config": {"num_instances": self.num_workers},
                "secondary_worker_config": {"num_instances": self.num_preemptible_workers},
            }
        }
        return scale_data

    @property
    def _graceful_decommission_timeout_object(self) -> dict[str, int] | None:
        if not self.graceful_decommission_timeout:
            return None

        timeout = None
        match = re.fullmatch(r"(\d+)([smdh])", self.graceful_decommission_timeout)
        if match:
            val = int(match.group(1))
            unit = match.group(2)
            if unit == "s":
                timeout = val
            elif unit == "m":
                timeout = int(timedelta(minutes=val).total_seconds())
            elif unit == "h":
                timeout = int(timedelta(hours=val).total_seconds())
            elif unit == "d":
                timeout = int(timedelta(days=val).total_seconds())

        if not timeout:
            raise AirflowException(
                "DataprocClusterScaleOperator "
                " should be expressed in day, hours, minutes or seconds. "
                " i.e. 1d, 4h, 10m, 30s"
            )

        return {"seconds": timeout}

    def execute(self, context: Context) -> None:
        """Scale, up or down, a cluster on Google Cloud Dataproc."""
        self.log.info("Scaling cluster: %s", self.cluster_name)

        scaling_cluster_data = self._build_scale_cluster_data()
        update_mask = ["config.worker_config.num_instances", "config.secondary_worker_config.num_instances"]

        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        # Save data required to display extra link no matter what the cluster status will be
        DataprocLink.persist(
            context=context,
            task_instance=self,
            url=DATAPROC_CLUSTER_LINK_DEPRECATED,
            resource=self.cluster_name,
        )
        operation = hook.update_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
            cluster=scaling_cluster_data,
            graceful_decommission_timeout=self._graceful_decommission_timeout_object,
            update_mask={"paths": update_mask},
        )
        operation.result()
        self.log.info("Cluster scaling finished")


class DataprocDeleteClusterOperator(GoogleCloudBaseOperator):
    """Delete a cluster in a project.

    :param region: Required. The Cloud Dataproc region in which to handle the request (templated).
    :param cluster_name: Required. The cluster name (templated).
    :param project_id: Optional. The ID of the Google Cloud project that the cluster belongs to (templated).
    :param cluster_uuid: Optional. Specifying the ``cluster_uuid`` means the RPC should fail
        if cluster with specified UUID does not exist.
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``DeleteClusterRequest`` requests with the same id, then the second request will be ignored and the
        first ``google.longrunning.Operation`` created and stored in the backend is returned.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run operator in the deferrable mode.
    :param polling_interval_seconds: Time (seconds) to wait between calls to check the cluster status.
    """

    template_fields: Sequence[str] = ("project_id", "region", "cluster_name", "impersonation_chain")

    def __init__(
        self,
        *,
        region: str,
        cluster_name: str,
        project_id: str | None = None,
        cluster_uuid: str | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float = 1 * 60 * 60,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        polling_interval_seconds: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if deferrable and polling_interval_seconds <= 0:
            raise ValueError("Invalid value for polling_interval_seconds. Expected value greater than 0")
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.cluster_uuid = cluster_uuid
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.polling_interval_seconds = polling_interval_seconds

    def execute(self, context: Context) -> None:
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        operation = self._delete_cluster(hook)
        if not self.deferrable:
            hook.wait_for_operation(timeout=self.timeout, result_retry=self.retry, operation=operation)
            self.log.info("Cluster deleted.")
        else:
            end_time: float = time.time() + self.timeout
            self.defer(
                trigger=DataprocDeleteClusterTrigger(
                    gcp_conn_id=self.gcp_conn_id,
                    project_id=self.project_id,
                    region=self.region,
                    cluster_name=self.cluster_name,
                    end_time=end_time,
                    metadata=self.metadata,
                    impersonation_chain=self.impersonation_chain,
                    polling_interval_seconds=self.polling_interval_seconds,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> Any:
        """
        Callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event and event["status"] == "error":
            raise AirflowException(event["message"])
        elif event is None:
            raise AirflowException("No event received in trigger callback")
        self.log.info("Cluster deleted.")

    def _delete_cluster(self, hook: DataprocHook):
        self.log.info("Deleting cluster: %s", self.cluster_name)
        return hook.delete_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
            cluster_uuid=self.cluster_uuid,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocJobBaseOperator(GoogleCloudBaseOperator):
    """Base class for operators that launch job on DataProc.

    :param region: The specified region where the dataproc cluster is created.
    :param job_name: The job name used in the DataProc cluster. This name by default
        is the task_id appended with the execution data, but can be templated. The
        name will always be appended with a random number to avoid name clashes.
    :param cluster_name: The name of the DataProc cluster.
    :param project_id: The ID of the Google Cloud project the cluster belongs to,
        if not specified the project will be inferred from the provided GCP connection.
    :param dataproc_properties: Map for the Hive properties. Ideal to put in
        default arguments (templated)
    :param dataproc_jars: HCFS URIs of jar files to add to the CLASSPATH of the Hive server and Hadoop
        MapReduce (MR) tasks. Can contain Hive SerDes and UDFs. (templated)
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param labels: The labels to associate with this job. Label keys must contain 1 to 63 characters,
        and must conform to RFC 1035. Label values may be empty, but, if present, must contain 1 to 63
        characters, and must conform to RFC 1035. No more than 32 labels can be associated with a job.
    :param job_error_states: Job states that should be considered error states.
        Any states in this set will result in an error being raised and failure of the
        task. Eg, if the ``CANCELLED`` state should also be considered a task failure,
        pass in ``{'ERROR', 'CANCELLED'}``. Possible values are currently only
        ``'ERROR'`` and ``'CANCELLED'``, but could change in the future. Defaults to
        ``{'ERROR'}``.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param asynchronous: Flag to return after submitting the job to the Dataproc API.
        This is useful for submitting long running jobs and
        waiting on them asynchronously using the DataprocJobSensor
    :param deferrable: Run operator in the deferrable mode
    :param polling_interval_seconds: time in seconds between polling for job completion.
        The value is considered only when running in deferrable mode. Must be greater than 0.

    :var dataproc_job_id: The actual "jobId" as submitted to the Dataproc API.
        This is useful for identifying or linking to the job in the Google Cloud Console
        Dataproc UI, as the actual "jobId" submitted to the Dataproc API is appended with
        an 8 character random string.
    :vartype dataproc_job_id: str
    """

    job_type = ""

    operator_extra_links = (DataprocLink(),)

    def __init__(
        self,
        *,
        region: str,
        job_name: str = "{{task.task_id}}_{{ds_nodash}}",
        cluster_name: str = "cluster-1",
        project_id: str | None = None,
        dataproc_properties: dict | None = None,
        dataproc_jars: list[str] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        labels: dict | None = None,
        job_error_states: set[str] | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        asynchronous: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        polling_interval_seconds: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if deferrable and polling_interval_seconds <= 0:
            raise ValueError("Invalid value for polling_interval_seconds. Expected value greater than 0")
        self.gcp_conn_id = gcp_conn_id
        self.labels = labels
        self.job_name = job_name
        self.cluster_name = cluster_name
        self.dataproc_properties = dataproc_properties
        self.dataproc_jars = dataproc_jars
        self.region = region

        self.job_error_states = job_error_states or {"ERROR"}
        self.impersonation_chain = impersonation_chain
        self.hook = DataprocHook(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)
        self.project_id = project_id or self.hook.project_id
        self.job_template: DataProcJobBuilder | None = None
        self.job: dict | None = None
        self.dataproc_job_id = None
        self.asynchronous = asynchronous
        self.deferrable = deferrable
        self.polling_interval_seconds = polling_interval_seconds

    def create_job_template(self) -> DataProcJobBuilder:
        """Initialize `self.job_template` with default values."""
        if self.project_id is None:
            raise AirflowException(
                "project id should either be set via project_id "
                "parameter or retrieved from the connection,"
            )
        job_template = DataProcJobBuilder(
            project_id=self.project_id,
            task_id=self.task_id,
            cluster_name=self.cluster_name,
            job_type=self.job_type,
            properties=self.dataproc_properties,
        )
        job_template.set_job_name(self.job_name)
        job_template.add_jar_file_uris(self.dataproc_jars)
        job_template.add_labels(self.labels)
        self.job_template = job_template
        return job_template

    def _generate_job_template(self) -> str:
        if self.job_template:
            job = self.job_template.build()
            return job["job"]
        raise Exception("Create a job template before")

    def execute(self, context: Context):
        if self.job_template:
            self.job = self.job_template.build()
            if self.job is None:
                raise Exception("The job should be set here.")
            self.dataproc_job_id = self.job["job"]["reference"]["job_id"]
            self.log.info("Submitting %s job %s", self.job_type, self.dataproc_job_id)
            job_object = self.hook.submit_job(
                project_id=self.project_id, job=self.job["job"], region=self.region
            )
            job_id = job_object.reference.job_id
            self.log.info("Job %s submitted successfully.", job_id)
            # Save data required for extra links no matter what the job status will be
            DataprocLink.persist(
                context=context, task_instance=self, url=DATAPROC_JOB_LINK_DEPRECATED, resource=job_id
            )

            if self.deferrable:
                self.defer(
                    trigger=DataprocSubmitTrigger(
                        job_id=job_id,
                        project_id=self.project_id,
                        region=self.region,
                        gcp_conn_id=self.gcp_conn_id,
                        impersonation_chain=self.impersonation_chain,
                        polling_interval_seconds=self.polling_interval_seconds,
                    ),
                    method_name="execute_complete",
                )
            if not self.asynchronous:
                self.log.info("Waiting for job %s to complete", job_id)
                self.hook.wait_for_job(job_id=job_id, region=self.region, project_id=self.project_id)
                self.log.info("Job %s completed successfully.", job_id)
            return job_id
        else:
            raise AirflowException("Create a job template before")

    def execute_complete(self, context, event=None) -> None:
        """
        Callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        job_state = event["job_state"]
        job_id = event["job_id"]
        if job_state == JobStatus.State.ERROR:
            raise AirflowException(f"Job failed:\n{job_id}")
        if job_state == JobStatus.State.CANCELLED:
            raise AirflowException(f"Job was cancelled:\n{job_id}")
        self.log.info("%s completed successfully.", self.task_id)
        return job_id

    def on_kill(self) -> None:
        """Callback called when the operator is killed; cancel any running job."""
        if self.dataproc_job_id:
            self.hook.cancel_job(project_id=self.project_id, job_id=self.dataproc_job_id, region=self.region)


class DataprocSubmitPigJobOperator(DataprocJobBaseOperator):
    """Start a Pig query Job on a Cloud DataProc cluster.

    .. seealso::
        This operator is deprecated, please use
        :class:`~airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`:

    The parameters of the operation will be passed to the cluster.

    It's a good practice to define dataproc_* parameters in the default_args of the dag
    like the cluster name and UDFs.

    .. code-block:: python

        default_args = {
            "cluster_name": "cluster-1",
            "dataproc_pig_jars": [
                "gs://example/udf/jar/datafu/1.2.0/datafu.jar",
                "gs://example/udf/jar/gpig/1.2/gpig.jar",
            ],
        }

    You can pass a pig script as string or file reference. Use variables to pass on
    variables for the pig script to be resolved on the cluster or use the parameters to
    be resolved in the script as template parameters.

    .. code-block:: python

        t1 = DataProcPigOperator(
            task_id="dataproc_pig",
            query="a_pig_script.pig",
            variables={"out": "gs://example/output/{{ds}}"},
        )

    .. seealso::
        For more detail on about job submission have a look at the reference:
        https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs

    :param query: The query or reference to the query
        file (pg or pig extension). (templated)
    :param query_uri: The HCFS URI of the script that contains the Pig queries.
    :param variables: Map of named parameters for the query. (templated)
    """

    template_fields: Sequence[str] = (
        "query",
        "variables",
        "job_name",
        "cluster_name",
        "region",
        "dataproc_jars",
        "dataproc_properties",
        "impersonation_chain",
    )
    template_ext = (".pg", ".pig")
    ui_color = "#0273d4"
    job_type = "pig_job"

    operator_extra_links = (DataprocLink(),)

    def __init__(
        self,
        *,
        query: str | None = None,
        query_uri: str | None = None,
        variables: dict | None = None,
        **kwargs,
    ) -> None:
        # TODO: Remove one day
        warnings.warn(
            "The `{cls}` operator is deprecated, please use `DataprocSubmitJobOperator` instead. You can use"
            " `generate_job` method of `{cls}` to generate dictionary representing your job"
            " and use it with the new operator.".format(cls=type(self).__name__),
            AirflowProviderDeprecationWarning,
            stacklevel=1,
        )

        super().__init__(**kwargs)
        self.query = query
        self.query_uri = query_uri
        self.variables = variables

    def generate_job(self):
        """
        Helper method for easier migration to `DataprocSubmitJobOperator`.

        :return: Dict representing Dataproc job
        """
        job_template = self.create_job_template()

        if self.query is None:
            if self.query_uri is None:
                raise AirflowException("One of query or query_uri should be set here")
            job_template.add_query_uri(self.query_uri)
        else:
            job_template.add_query(self.query)
        job_template.add_variables(self.variables)
        return self._generate_job_template()

    def execute(self, context: Context):
        job_template = self.create_job_template()
        if self.query is None:
            if self.query_uri is None:
                raise AirflowException("One of query or query_uri should be set here")
            job_template.add_query_uri(self.query_uri)
        else:
            job_template.add_query(self.query)
        job_template.add_variables(self.variables)

        super().execute(context)


class DataprocSubmitHiveJobOperator(DataprocJobBaseOperator):
    """Start a Hive query Job on a Cloud DataProc cluster.

    .. seealso::
        This operator is deprecated, please use
        :class:`~airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`:

    :param query: The query or reference to the query file (q extension).
    :param query_uri: The HCFS URI of the script that contains the Hive queries.
    :param variables: Map of named parameters for the query.
    """

    template_fields: Sequence[str] = (
        "query",
        "variables",
        "job_name",
        "cluster_name",
        "region",
        "dataproc_jars",
        "dataproc_properties",
        "impersonation_chain",
    )
    template_ext = (".q", ".hql")
    ui_color = "#0273d4"
    job_type = "hive_job"

    def __init__(
        self,
        *,
        query: str | None = None,
        query_uri: str | None = None,
        variables: dict | None = None,
        **kwargs,
    ) -> None:
        # TODO: Remove one day
        warnings.warn(
            "The `{cls}` operator is deprecated, please use `DataprocSubmitJobOperator` instead. You can use"
            " `generate_job` method of `{cls}` to generate dictionary representing your job"
            " and use it with the new operator.".format(cls=type(self).__name__),
            AirflowProviderDeprecationWarning,
            stacklevel=1,
        )

        super().__init__(**kwargs)
        self.query = query
        self.query_uri = query_uri
        self.variables = variables
        if self.query is not None and self.query_uri is not None:
            raise AirflowException("Only one of `query` and `query_uri` can be passed.")

    def generate_job(self):
        """
        Helper method for easier migration to `DataprocSubmitJobOperator`.

        :return: Dict representing Dataproc job
        """
        job_template = self.create_job_template()
        if self.query is None:
            if self.query_uri is None:
                raise AirflowException("One of query or query_uri should be set here")
            job_template.add_query_uri(self.query_uri)
        else:
            job_template.add_query(self.query)
        job_template.add_variables(self.variables)
        return self._generate_job_template()

    def execute(self, context: Context):
        job_template = self.create_job_template()
        if self.query is None:
            if self.query_uri is None:
                raise AirflowException("One of query or query_uri should be set here")
            job_template.add_query_uri(self.query_uri)
        else:
            job_template.add_query(self.query)
        job_template.add_variables(self.variables)
        super().execute(context)


class DataprocSubmitSparkSqlJobOperator(DataprocJobBaseOperator):
    """Start a Spark SQL query Job on a Cloud DataProc cluster.

    .. seealso::
        This operator is deprecated, please use
        :class:`~airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`:

    :param query: The query or reference to the query file (q extension). (templated)
    :param query_uri: The HCFS URI of the script that contains the SQL queries.
    :param variables: Map of named parameters for the query. (templated)
    """

    template_fields: Sequence[str] = (
        "query",
        "variables",
        "job_name",
        "cluster_name",
        "region",
        "dataproc_jars",
        "dataproc_properties",
        "impersonation_chain",
    )
    template_ext = (".q",)
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#0273d4"
    job_type = "spark_sql_job"

    def __init__(
        self,
        *,
        query: str | None = None,
        query_uri: str | None = None,
        variables: dict | None = None,
        **kwargs,
    ) -> None:
        # TODO: Remove one day
        warnings.warn(
            "The `{cls}` operator is deprecated, please use `DataprocSubmitJobOperator` instead. You can use"
            " `generate_job` method of `{cls}` to generate dictionary representing your job"
            " and use it with the new operator.".format(cls=type(self).__name__),
            AirflowProviderDeprecationWarning,
            stacklevel=1,
        )

        super().__init__(**kwargs)
        self.query = query
        self.query_uri = query_uri
        self.variables = variables
        if self.query is not None and self.query_uri is not None:
            raise AirflowException("Only one of `query` and `query_uri` can be passed.")

    def generate_job(self):
        """
        Helper method for easier migration to `DataprocSubmitJobOperator`.

        :return: Dict representing Dataproc job
        """
        job_template = self.create_job_template()
        if self.query is None:
            job_template.add_query_uri(self.query_uri)
        else:
            job_template.add_query(self.query)
        job_template.add_variables(self.variables)
        return self._generate_job_template()

    def execute(self, context: Context):
        job_template = self.create_job_template()
        if self.query is None:
            if self.query_uri is None:
                raise AirflowException("One of query or query_uri should be set here")
            job_template.add_query_uri(self.query_uri)
        else:
            job_template.add_query(self.query)
        job_template.add_variables(self.variables)
        super().execute(context)


class DataprocSubmitSparkJobOperator(DataprocJobBaseOperator):
    """Start a Spark Job on a Cloud DataProc cluster.

    .. seealso::
        This operator is deprecated, please use
        :class:`~airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`:

    :param main_jar: The HCFS URI of the jar file that contains the main class
        (use this or the main_class, not both together).
    :param main_class: Name of the job class. (use this or the main_jar, not both
        together).
    :param arguments: Arguments for the job. (templated)
    :param archives: List of archived files that will be unpacked in the work
        directory. Should be stored in Cloud Storage.
    :param files: List of files to be copied to the working directory
    """

    template_fields: Sequence[str] = (
        "arguments",
        "job_name",
        "cluster_name",
        "region",
        "dataproc_jars",
        "dataproc_properties",
        "impersonation_chain",
    )
    ui_color = "#0273d4"
    job_type = "spark_job"

    def __init__(
        self,
        *,
        main_jar: str | None = None,
        main_class: str | None = None,
        arguments: list | None = None,
        archives: list | None = None,
        files: list | None = None,
        **kwargs,
    ) -> None:
        # TODO: Remove one day
        warnings.warn(
            "The `{cls}` operator is deprecated, please use `DataprocSubmitJobOperator` instead. You can use"
            " `generate_job` method of `{cls}` to generate dictionary representing your job"
            " and use it with the new operator.".format(cls=type(self).__name__),
            AirflowProviderDeprecationWarning,
            stacklevel=1,
        )

        super().__init__(**kwargs)
        self.main_jar = main_jar
        self.main_class = main_class
        self.arguments = arguments
        self.archives = archives
        self.files = files

    def generate_job(self):
        """
        Helper method for easier migration to `DataprocSubmitJobOperator`.

        :return: Dict representing Dataproc job
        """
        job_template = self.create_job_template()
        job_template.set_main(self.main_jar, self.main_class)
        job_template.add_args(self.arguments)
        job_template.add_archive_uris(self.archives)
        job_template.add_file_uris(self.files)
        return self._generate_job_template()

    def execute(self, context: Context):
        job_template = self.create_job_template()
        job_template.set_main(self.main_jar, self.main_class)
        job_template.add_args(self.arguments)
        job_template.add_archive_uris(self.archives)
        job_template.add_file_uris(self.files)
        super().execute(context)


class DataprocSubmitHadoopJobOperator(DataprocJobBaseOperator):
    """Start a Hadoop Job on a Cloud DataProc cluster.

    .. seealso::
        This operator is deprecated, please use
        :class:`~airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`:

    :param main_jar: The HCFS URI of the jar file containing the main class
        (use this or the main_class, not both together).
    :param main_class: Name of the job class. (use this or the main_jar, not both
        together).
    :param arguments: Arguments for the job. (templated)
    :param archives: List of archived files that will be unpacked in the work
        directory. Should be stored in Cloud Storage.
    :param files: List of files to be copied to the working directory
    """

    template_fields: Sequence[str] = (
        "arguments",
        "job_name",
        "cluster_name",
        "region",
        "dataproc_jars",
        "dataproc_properties",
        "impersonation_chain",
    )
    ui_color = "#0273d4"
    job_type = "hadoop_job"

    def __init__(
        self,
        *,
        main_jar: str | None = None,
        main_class: str | None = None,
        arguments: list | None = None,
        archives: list | None = None,
        files: list | None = None,
        **kwargs,
    ) -> None:
        # TODO: Remove one day
        warnings.warn(
            "The `{cls}` operator is deprecated, please use `DataprocSubmitJobOperator` instead. You can use"
            " `generate_job` method of `{cls}` to generate dictionary representing your job"
            " and use it with the new operator.".format(cls=type(self).__name__),
            AirflowProviderDeprecationWarning,
            stacklevel=1,
        )

        super().__init__(**kwargs)
        self.main_jar = main_jar
        self.main_class = main_class
        self.arguments = arguments
        self.archives = archives
        self.files = files

    def generate_job(self):
        """Helper method for easier migration to `DataprocSubmitJobOperator`.

        :return: Dict representing Dataproc job
        """
        job_template = self.create_job_template()
        job_template.set_main(self.main_jar, self.main_class)
        job_template.add_args(self.arguments)
        job_template.add_archive_uris(self.archives)
        job_template.add_file_uris(self.files)
        return self._generate_job_template()

    def execute(self, context: Context):
        job_template = self.create_job_template()
        job_template.set_main(self.main_jar, self.main_class)
        job_template.add_args(self.arguments)
        job_template.add_archive_uris(self.archives)
        job_template.add_file_uris(self.files)
        super().execute(context)


class DataprocSubmitPySparkJobOperator(DataprocJobBaseOperator):
    """Start a PySpark Job on a Cloud DataProc cluster.

    .. seealso::
        This operator is deprecated, please use
        :class:`~airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`:

    :param main: [Required] The Hadoop Compatible Filesystem (HCFS) URI of the main
            Python file to use as the driver. Must be a .py file. (templated)
    :param arguments: Arguments for the job. (templated)
    :param archives: List of archived files that will be unpacked in the work
        directory. Should be stored in Cloud Storage.
    :param files: List of files to be copied to the working directory
    :param pyfiles: List of Python files to pass to the PySpark framework.
        Supported file types: .py, .egg, and .zip
    """

    template_fields: Sequence[str] = (
        "main",
        "arguments",
        "job_name",
        "cluster_name",
        "region",
        "dataproc_jars",
        "dataproc_properties",
        "impersonation_chain",
    )
    ui_color = "#0273d4"
    job_type = "pyspark_job"

    @staticmethod
    def _generate_temp_filename(filename):
        return f"{time:%Y%m%d%H%M%S}_{uuid.uuid4()!s:.8}_{ntpath.basename(filename)}"

    def _upload_file_temp(self, bucket, local_file):
        """Upload a local file to a Google Cloud Storage bucket."""
        temp_filename = self._generate_temp_filename(local_file)
        if not bucket:
            raise AirflowException(
                "If you want Airflow to upload the local file to a temporary bucket, set "
                "the 'temp_bucket' key in the connection string"
            )

        self.log.info("Uploading %s to %s", local_file, temp_filename)

        GCSHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain).upload(
            bucket_name=bucket,
            object_name=temp_filename,
            mime_type="application/x-python",
            filename=local_file,
        )
        return f"gs://{bucket}/{temp_filename}"

    def __init__(
        self,
        *,
        main: str,
        arguments: list | None = None,
        archives: list | None = None,
        pyfiles: list | None = None,
        files: list | None = None,
        **kwargs,
    ) -> None:
        # TODO: Remove one day
        warnings.warn(
            "The `{cls}` operator is deprecated, please use `DataprocSubmitJobOperator` instead. You can use"
            " `generate_job` method of `{cls}` to generate dictionary representing your job"
            " and use it with the new operator.".format(cls=type(self).__name__),
            AirflowProviderDeprecationWarning,
            stacklevel=1,
        )

        super().__init__(**kwargs)
        self.main = main
        self.arguments = arguments
        self.archives = archives
        self.files = files
        self.pyfiles = pyfiles

    def generate_job(self):
        """Helper method for easier migration to :class:`DataprocSubmitJobOperator`.

        :return: Dict representing Dataproc job
        """
        job_template = self.create_job_template()
        #  Check if the file is local, if that is the case, upload it to a bucket
        if os.path.isfile(self.main):
            cluster_info = self.hook.get_cluster(
                project_id=self.project_id, region=self.region, cluster_name=self.cluster_name
            )
            bucket = cluster_info["config"]["config_bucket"]
            self.main = f"gs://{bucket}/{self.main}"
        job_template.set_python_main(self.main)
        job_template.add_args(self.arguments)
        job_template.add_archive_uris(self.archives)
        job_template.add_file_uris(self.files)
        job_template.add_python_file_uris(self.pyfiles)

        return self._generate_job_template()

    def execute(self, context: Context):
        job_template = self.create_job_template()
        #  Check if the file is local, if that is the case, upload it to a bucket
        if os.path.isfile(self.main):
            cluster_info = self.hook.get_cluster(
                project_id=self.project_id, region=self.region, cluster_name=self.cluster_name
            )
            bucket = cluster_info["config"]["config_bucket"]
            self.main = self._upload_file_temp(bucket, self.main)

        job_template.set_python_main(self.main)
        job_template.add_args(self.arguments)
        job_template.add_archive_uris(self.archives)
        job_template.add_file_uris(self.files)
        job_template.add_python_file_uris(self.pyfiles)
        super().execute(context)


class DataprocCreateWorkflowTemplateOperator(GoogleCloudBaseOperator):
    """Creates new workflow template.

    :param project_id: Optional. The ID of the Google Cloud project the cluster belongs to.
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param template: The Dataproc workflow template to create. If a dict is provided,
        it must be of the same form as the protobuf message WorkflowTemplate.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    """

    template_fields: Sequence[str] = ("region", "template")
    template_fields_renderers = {"template": "json"}
    operator_extra_links = (DataprocWorkflowTemplateLink(),)

    def __init__(
        self,
        *,
        template: dict,
        region: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.region = region
        self.template = template
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Creating template")
        try:
            workflow = hook.create_workflow_template(
                region=self.region,
                template=self.template,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Workflow %s created", workflow.name)
        except AlreadyExists:
            self.log.info("Workflow with given id already exists")
        project_id = self.project_id or hook.project_id
        if project_id:
            DataprocWorkflowTemplateLink.persist(
                context=context,
                operator=self,
                workflow_template_id=self.template["id"],
                region=self.region,
                project_id=project_id,
            )


class DataprocInstantiateWorkflowTemplateOperator(GoogleCloudBaseOperator):
    """Instantiate a WorkflowTemplate on Google Cloud Dataproc.

    The operator will wait until the WorkflowTemplate is finished executing.

    .. seealso::
        Please refer to:
        https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.workflowTemplates/instantiate

    :param template_id: The id of the template. (templated)
    :param project_id: The ID of the google cloud project in which
        the template runs
    :param region: The specified region where the dataproc cluster is created.
    :param parameters: a map of parameters for Dataproc Template in key-value format:
        map (key: string, value: string)
        Example: { "date_from": "2019-08-01", "date_to": "2019-08-02"}.
        Values may not exceed 100 characters. Please refer to:
        https://cloud.google.com/dataproc/docs/concepts/workflows/workflow-parameters
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``SubmitJobRequest`` requests with the same id, then the second request will be ignored and the first
        ``Job`` created and stored in the backend is returned.
        It is recommended to always set this value to a UUID.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run operator in the deferrable mode.
    :param polling_interval_seconds: Time (seconds) to wait between calls to check the run status.
    :param cancel_on_kill: Flag which indicates whether cancel the workflow, when on_kill is called
    """

    template_fields: Sequence[str] = ("template_id", "impersonation_chain", "request_id", "parameters")
    template_fields_renderers = {"parameters": "json"}
    operator_extra_links = (DataprocWorkflowLink(),)

    def __init__(
        self,
        *,
        template_id: str,
        region: str,
        project_id: str | None = None,
        version: int | None = None,
        request_id: str | None = None,
        parameters: dict[str, str] | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        polling_interval_seconds: int = 10,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if deferrable and polling_interval_seconds <= 0:
            raise ValueError("Invalid value for polling_interval_seconds. Expected value greater than 0")
        self.template_id = template_id
        self.parameters = parameters
        self.version = version
        self.project_id = project_id
        self.region = region
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.request_id = request_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.polling_interval_seconds = polling_interval_seconds
        self.cancel_on_kill = cancel_on_kill
        self.operation_name: str | None = None

    def execute(self, context: Context):
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Instantiating template %s", self.template_id)
        operation = hook.instantiate_workflow_template(
            project_id=self.project_id,
            region=self.region,
            template_name=self.template_id,
            version=self.version,
            request_id=self.request_id,
            parameters=self.parameters,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        operation_name = operation.operation.name
        self.operation_name = operation_name
        workflow_id = operation_name.split("/")[-1]
        project_id = self.project_id or hook.project_id
        if project_id:
            DataprocWorkflowLink.persist(
                context=context,
                operator=self,
                workflow_id=workflow_id,
                region=self.region,
                project_id=project_id,
            )
        self.log.info("Template instantiated. Workflow Id : %s", workflow_id)
        if not self.deferrable:
            hook.wait_for_operation(timeout=self.timeout, result_retry=self.retry, operation=operation)
            self.log.info("Workflow %s completed successfully", workflow_id)
        else:
            self.defer(
                trigger=DataprocWorkflowTrigger(
                    name=operation_name,
                    project_id=self.project_id,
                    region=self.region,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    polling_interval_seconds=self.polling_interval_seconds,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context, event=None) -> None:
        """Callback for when the trigger fires.

        This returns immediately. It relies on trigger to throw an exception,
        otherwise it assumes execution was successful.
        """
        if event["status"] in ("failed", "error"):
            self.log.exception("Unexpected error in the operation.")
            raise AirflowException(event["message"])

        self.log.info("Workflow %s completed successfully", event["operation_name"])

    def on_kill(self) -> None:
        if self.cancel_on_kill and self.operation_name:
            hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
            hook.get_operations_client(region=self.region).cancel_operation(name=self.operation_name)


class DataprocInstantiateInlineWorkflowTemplateOperator(GoogleCloudBaseOperator):
    """Instantiate a WorkflowTemplate Inline on Google Cloud Dataproc.

    The operator will wait until the WorkflowTemplate is finished executing.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataprocInstantiateInlineWorkflowTemplateOperator`

        For more detail on about instantiate inline have a look at the reference:
        https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.workflowTemplates/instantiateInline

    :param template: The template contents. (templated)
    :param project_id: The ID of the google cloud project in which
        the template runs
    :param region: The specified region where the dataproc cluster is created.
    :param parameters: a map of parameters for Dataproc Template in key-value format:
        map (key: string, value: string)
        Example: { "date_from": "2019-08-01", "date_to": "2019-08-02"}.
        Values may not exceed 100 characters. Please refer to:
        https://cloud.google.com/dataproc/docs/concepts/workflows/workflow-parameters
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``SubmitJobRequest`` requests with the same id, then the second request will be ignored and the first
        ``Job`` created and stored in the backend is returned.
        It is recommended to always set this value to a UUID.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run operator in the deferrable mode.
    :param polling_interval_seconds: Time (seconds) to wait between calls to check the run status.
    :param cancel_on_kill: Flag which indicates whether cancel the workflow, when on_kill is called
    """

    template_fields: Sequence[str] = ("template", "impersonation_chain")
    template_fields_renderers = {"template": "json"}
    operator_extra_links = (DataprocWorkflowLink(),)

    def __init__(
        self,
        *,
        template: dict,
        region: str,
        project_id: str | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        polling_interval_seconds: int = 10,
        cancel_on_kill: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if deferrable and polling_interval_seconds <= 0:
            raise ValueError("Invalid value for polling_interval_seconds. Expected value greater than 0")
        self.template = template
        self.project_id = project_id
        self.region = region
        self.template = template
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.polling_interval_seconds = polling_interval_seconds
        self.cancel_on_kill = cancel_on_kill
        self.operation_name: str | None = None

    def execute(self, context: Context):
        self.log.info("Instantiating Inline Template")
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        project_id = self.project_id or hook.project_id
        operation = hook.instantiate_inline_workflow_template(
            template=self.template,
            project_id=project_id,
            region=self.region,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        operation_name = operation.operation.name
        self.operation_name = operation_name
        workflow_id = operation_name.split("/")[-1]
        if project_id:
            DataprocWorkflowLink.persist(
                context=context,
                operator=self,
                workflow_id=workflow_id,
                region=self.region,
                project_id=project_id,
            )
        if not self.deferrable:
            self.log.info("Template instantiated. Workflow Id : %s", workflow_id)
            operation.result()
            self.log.info("Workflow %s completed successfully", workflow_id)
        else:
            self.defer(
                trigger=DataprocWorkflowTrigger(
                    name=operation_name,
                    project_id=self.project_id or hook.project_id,
                    region=self.region,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    polling_interval_seconds=self.polling_interval_seconds,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context, event=None) -> None:
        """Callback for when the trigger fires.

        This returns immediately. It relies on trigger to throw an exception,
        otherwise it assumes execution was successful.
        """
        if event["status"] in ("failed", "error"):
            self.log.exception("Unexpected error in the operation.")
            raise AirflowException(event["message"])

        self.log.info("Workflow %s completed successfully", event["operation_name"])

    def on_kill(self) -> None:
        if self.cancel_on_kill and self.operation_name:
            hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
            hook.get_operations_client(region=self.region).cancel_operation(name=self.operation_name)


class DataprocSubmitJobOperator(GoogleCloudBaseOperator):
    """Submit a job to a cluster.

    :param project_id: Optional. The ID of the Google Cloud project that the job belongs to.
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param job: Required. The job resource.
        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.dataproc_v1.types.Job`.
        For the complete list of supported job types and their configurations please take a look here
        https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``SubmitJobRequest`` requests with the same id, then the second request will be ignored and the first
        ``Job`` created and stored in the backend is returned.
        It is recommended to always set this value to a UUID.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id:
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param asynchronous: Flag to return after submitting the job to the Dataproc API.
        This is useful for submitting long running jobs and
        waiting on them asynchronously using the DataprocJobSensor
    :param deferrable: Run operator in the deferrable mode
    :param polling_interval_seconds: time in seconds between polling for job completion.
        The value is considered only when running in deferrable mode. Must be greater than 0.
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    :param wait_timeout: How many seconds wait for job to be ready. Used only if ``asynchronous`` is False
    """

    template_fields: Sequence[str] = ("project_id", "region", "job", "impersonation_chain", "request_id")
    template_fields_renderers = {"job": "json"}

    operator_extra_links = (DataprocJobLink(),)

    def __init__(
        self,
        *,
        job: dict,
        region: str,
        project_id: str | None = None,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        asynchronous: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        polling_interval_seconds: int = 10,
        cancel_on_kill: bool = True,
        wait_timeout: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if deferrable and polling_interval_seconds <= 0:
            raise ValueError("Invalid value for polling_interval_seconds. Expected value greater than 0")
        self.project_id = project_id
        self.region = region
        self.job = job
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.asynchronous = asynchronous
        self.deferrable = deferrable
        self.polling_interval_seconds = polling_interval_seconds
        self.cancel_on_kill = cancel_on_kill
        self.hook: DataprocHook | None = None
        self.job_id: str | None = None
        self.wait_timeout = wait_timeout

    def execute(self, context: Context):
        self.log.info("Submitting job")
        self.hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        job_object = self.hook.submit_job(
            project_id=self.project_id,
            region=self.region,
            job=self.job,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        new_job_id: str = job_object.reference.job_id
        self.log.info("Job %s submitted successfully.", new_job_id)
        # Save data required by extra links no matter what the job status will be
        project_id = self.project_id or self.hook.project_id
        if project_id:
            DataprocJobLink.persist(
                context=context,
                operator=self,
                job_id=new_job_id,
                region=self.region,
                project_id=project_id,
            )

        self.job_id = new_job_id
        if self.deferrable:
            job = self.hook.get_job(project_id=self.project_id, region=self.region, job_id=self.job_id)
            state = job.status.state
            if state == JobStatus.State.DONE:
                return self.job_id
            elif state == JobStatus.State.ERROR:
                raise AirflowException(f"Job failed:\n{job}")
            elif state == JobStatus.State.CANCELLED:
                raise AirflowException(f"Job was cancelled:\n{job}")
            self.defer(
                trigger=DataprocSubmitTrigger(
                    job_id=self.job_id,
                    project_id=self.project_id,
                    region=self.region,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    polling_interval_seconds=self.polling_interval_seconds,
                ),
                method_name="execute_complete",
            )
        elif not self.asynchronous:
            self.log.info("Waiting for job %s to complete", new_job_id)
            self.hook.wait_for_job(
                job_id=new_job_id, region=self.region, project_id=self.project_id, timeout=self.wait_timeout
            )
            self.log.info("Job %s completed successfully.", new_job_id)
        return self.job_id

    def execute_complete(self, context, event=None) -> None:
        """Callback for when the trigger fires.

        This returns immediately. It relies on trigger to throw an exception,
        otherwise it assumes execution was successful.
        """
        job_state = event["job_state"]
        job_id = event["job_id"]
        if job_state == JobStatus.State.ERROR:
            raise AirflowException(f"Job failed:\n{job_id}")
        if job_state == JobStatus.State.CANCELLED:
            raise AirflowException(f"Job was cancelled:\n{job_id}")
        self.log.info("%s completed successfully.", self.task_id)
        return job_id

    def on_kill(self):
        if self.job_id and self.cancel_on_kill:
            self.hook.cancel_job(job_id=self.job_id, project_id=self.project_id, region=self.region)


class DataprocUpdateClusterOperator(GoogleCloudBaseOperator):
    """Update a cluster in a project.

    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param project_id: Optional. The ID of the Google Cloud project the cluster belongs to.
    :param cluster_name: Required. The cluster name.
    :param cluster: Required. The changes to the cluster.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.dataproc_v1.types.Cluster`
    :param update_mask: Required. Specifies the path, relative to ``Cluster``, of the field to update. For
        example, to change the number of workers in a cluster to 5, the ``update_mask`` parameter would be
        specified as ``config.worker_config.num_instances``, and the ``PATCH`` request body would specify the
        new value. If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.protobuf.field_mask_pb2.FieldMask`
    :param graceful_decommission_timeout: Optional. Timeout for graceful YARN decommissioning. Graceful
        decommissioning allows removing nodes from the cluster without interrupting jobs in progress. Timeout
        specifies how long to wait for jobs in progress to finish before forcefully removing nodes (and
        potentially interrupting jobs). Default timeout is 0 (for forceful decommission), and the maximum
        allowed timeout is 1 day.
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``UpdateClusterRequest`` requests with the same id, then the second request will be ignored and the
        first ``google.long-running.Operation`` created and stored in the backend is returned.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run operator in the deferrable mode.
    :param polling_interval_seconds: Time (seconds) to wait between calls to check the run status.
    """

    template_fields: Sequence[str] = (
        "cluster_name",
        "cluster",
        "region",
        "request_id",
        "project_id",
        "impersonation_chain",
    )
    operator_extra_links = (DataprocClusterLink(),)

    def __init__(
        self,
        *,
        cluster_name: str,
        cluster: dict | Cluster,
        update_mask: dict | FieldMask,
        graceful_decommission_timeout: dict | Duration,
        region: str,
        request_id: str | None = None,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        polling_interval_seconds: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if deferrable and polling_interval_seconds <= 0:
            raise ValueError("Invalid value for polling_interval_seconds. Expected value greater than 0")
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.cluster = cluster
        self.update_mask = update_mask
        self.graceful_decommission_timeout = graceful_decommission_timeout
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.polling_interval_seconds = polling_interval_seconds

    def execute(self, context: Context):
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        # Save data required by extra links no matter what the cluster status will be
        project_id = self.project_id or hook.project_id
        if project_id:
            DataprocClusterLink.persist(
                context=context,
                operator=self,
                cluster_id=self.cluster_name,
                project_id=project_id,
                region=self.region,
            )
        self.log.info("Updating %s cluster.", self.cluster_name)
        operation = hook.update_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
            cluster=self.cluster,
            update_mask=self.update_mask,
            graceful_decommission_timeout=self.graceful_decommission_timeout,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        if not self.deferrable:
            hook.wait_for_operation(timeout=self.timeout, result_retry=self.retry, operation=operation)
        else:
            self.defer(
                trigger=DataprocClusterTrigger(
                    cluster_name=self.cluster_name,
                    project_id=self.project_id,
                    region=self.region,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    polling_interval_seconds=self.polling_interval_seconds,
                ),
                method_name="execute_complete",
            )
        self.log.info("Updated %s cluster.", self.cluster_name)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        """
        Callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        cluster_state = event["cluster_state"]
        cluster_name = event["cluster_name"]

        if cluster_state == ClusterStatus.State.ERROR:
            raise AirflowException(f"Cluster is in ERROR state:\n{cluster_name}")
        self.log.info("%s completed successfully.", self.task_id)


class DataprocCreateBatchOperator(GoogleCloudBaseOperator):
    """Create a batch workload.

    :param project_id: Optional. The ID of the Google Cloud project that the cluster belongs to. (templated)
    :param region: Required. The Cloud Dataproc region in which to handle the request. (templated)
    :param batch: Required. The batch to create. (templated)
    :param batch_id: Optional. The ID to use for the batch, which will become the final component
        of the batch's resource name.
        This value must be 4-63 characters. Valid characters are /[a-z][0-9]-/. (templated)
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``CreateBatchRequest`` requests with the same id, then the second request will be ignored and
        the first ``google.longrunning.Operation`` created and stored in the backend is returned.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param result_retry: Result retry object used to retry requests. Is used to decrease delay between
        executing chained tasks in a DAG by specifying exact amount of seconds for executing.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param asynchronous: Flag to return after creating batch to the Dataproc API.
        This is useful for creating long-running batch and
        waiting on them asynchronously using the DataprocBatchSensor
    :param deferrable: Run operator in the deferrable mode.
    :param polling_interval_seconds: Time (seconds) to wait between calls to check the run status.
    """

    template_fields: Sequence[str] = (
        "project_id",
        "batch",
        "batch_id",
        "region",
        "impersonation_chain",
    )
    operator_extra_links = (DataprocBatchLink(),)

    def __init__(
        self,
        *,
        region: str | None = None,
        project_id: str | None = None,
        batch: dict | Batch,
        batch_id: str,
        request_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        result_retry: Retry | _MethodDefault = DEFAULT,
        asynchronous: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        polling_interval_seconds: int = 5,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if deferrable and polling_interval_seconds <= 0:
            raise ValueError("Invalid value for polling_interval_seconds. Expected value greater than 0")
        self.region = region
        self.project_id = project_id
        self.batch = batch
        self.batch_id = batch_id
        self.request_id = request_id
        self.retry = retry
        self.result_retry = result_retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.operation: operation.Operation | None = None
        self.asynchronous = asynchronous
        self.deferrable = deferrable
        self.polling_interval_seconds = polling_interval_seconds

    def execute(self, context: Context):
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        # batch_id might not be set and will be generated
        if self.batch_id:
            link = DATAPROC_BATCH_LINK.format(
                region=self.region, project_id=self.project_id, batch_id=self.batch_id
            )
            self.log.info("Creating batch %s", self.batch_id)
            self.log.info("Once started, the batch job will be available at %s", link)
        else:
            self.log.info("Starting batch job. The batch ID will be generated since it was not provided.")
        if self.region is None:
            raise AirflowException("Region should be set here")
        try:
            self.operation = hook.create_batch(
                region=self.region,
                project_id=self.project_id,
                batch=self.batch,
                batch_id=self.batch_id,
                request_id=self.request_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            if self.operation is None:
                raise RuntimeError("The operation should be set here!")

            if not self.deferrable:
                if not self.asynchronous:
                    result = hook.wait_for_operation(
                        timeout=self.timeout, result_retry=self.result_retry, operation=self.operation
                    )
                    self.log.info("Batch %s created", self.batch_id)

                else:
                    return self.operation.operation.name

            else:
                # processing ends in execute_complete
                self.defer(
                    trigger=DataprocBatchTrigger(
                        batch_id=self.batch_id,
                        project_id=self.project_id,
                        region=self.region,
                        gcp_conn_id=self.gcp_conn_id,
                        impersonation_chain=self.impersonation_chain,
                        polling_interval_seconds=self.polling_interval_seconds,
                    ),
                    method_name="execute_complete",
                )

        except AlreadyExists:
            self.log.info("Batch with given id already exists")
            # This is only likely to happen if batch_id was provided
            # Could be running if Airflow was restarted after task started
            # poll until a final state is reached

            self.log.info("Attaching to the job %s if it is still running.", self.batch_id)

            # deferrable handling of a batch_id that already exists - processing ends in execute_complete
            if self.deferrable:
                self.defer(
                    trigger=DataprocBatchTrigger(
                        batch_id=self.batch_id,
                        project_id=self.project_id,
                        region=self.region,
                        gcp_conn_id=self.gcp_conn_id,
                        impersonation_chain=self.impersonation_chain,
                        polling_interval_seconds=self.polling_interval_seconds,
                    ),
                    method_name="execute_complete",
                )

            # non-deferrable handling of a batch_id that already exists
            result = hook.wait_for_batch(
                batch_id=self.batch_id,
                region=self.region,
                project_id=self.project_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
                wait_check_interval=self.polling_interval_seconds,
            )
        batch_id = self.batch_id or result.name.split("/")[-1]

        self.handle_batch_status(context, result.state, batch_id)
        project_id = self.project_id or hook.project_id
        if project_id:
            DataprocBatchLink.persist(
                context=context,
                operator=self,
                project_id=project_id,
                region=self.region,
                batch_id=batch_id,
            )
        return Batch.to_dict(result)

    def execute_complete(self, context, event=None) -> None:
        """Callback for when the trigger fires.

        This returns immediately. It relies on trigger to throw an exception,
        otherwise it assumes execution was successful.
        """
        if event is None:
            raise AirflowException("Batch failed.")
        state = event["batch_state"]
        batch_id = event["batch_id"]
        self.handle_batch_status(context, state, batch_id)

    def on_kill(self):
        if self.operation:
            self.operation.cancel()

    def handle_batch_status(self, context: Context, state: Batch.State, batch_id: str) -> None:
        # The existing batch may be a number of states other than 'SUCCEEDED'\
        # wait_for_operation doesn't fail if the job is cancelled, so we will check for it here which also
        # finds a cancelling|canceled|unspecified job from wait_for_batch or the deferred trigger
        link = DATAPROC_BATCH_LINK.format(region=self.region, project_id=self.project_id, batch_id=batch_id)
        if state == Batch.State.FAILED:
            raise AirflowException("Batch job %s failed.  Driver Logs: %s", batch_id, link)
        if state in (Batch.State.CANCELLED, Batch.State.CANCELLING):
            raise AirflowException("Batch job %s was cancelled. Driver logs: %s", batch_id, link)
        if state == Batch.State.STATE_UNSPECIFIED:
            raise AirflowException("Batch job %s unspecified. Driver logs: %s", batch_id, link)
        self.log.info("Batch job %s completed. Driver logs: %s", batch_id, link)


class DataprocDeleteBatchOperator(GoogleCloudBaseOperator):
    """Delete the batch workload resource.

    :param batch_id: Required. The ID to use for the batch, which will become the final component
        of the batch's resource name.
        This value must be 4-63 characters. Valid characters are /[a-z][0-9]-/.
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param project_id: Optional. The ID of the Google Cloud project that the cluster belongs to.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = ("batch_id", "region", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        batch_id: str,
        region: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.batch_id = batch_id
        self.region = region
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Deleting batch: %s", self.batch_id)
        hook.delete_batch(
            batch_id=self.batch_id,
            region=self.region,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        self.log.info("Batch deleted.")


class DataprocGetBatchOperator(GoogleCloudBaseOperator):
    """Get the batch workload resource representation.

    :param batch_id: Required. The ID to use for the batch, which will become the final component
        of the batch's resource name.
        This value must be 4-63 characters. Valid characters are /[a-z][0-9]-/.
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param project_id: Optional. The ID of the Google Cloud project that the cluster belongs to.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = ("batch_id", "region", "project_id", "impersonation_chain")
    operator_extra_links = (DataprocBatchLink(),)

    def __init__(
        self,
        *,
        batch_id: str,
        region: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.batch_id = batch_id
        self.region = region
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        self.log.info("Getting batch: %s", self.batch_id)
        batch = hook.get_batch(
            batch_id=self.batch_id,
            region=self.region,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        project_id = self.project_id or hook.project_id
        if project_id:
            DataprocBatchLink.persist(
                context=context,
                operator=self,
                project_id=project_id,
                region=self.region,
                batch_id=self.batch_id,
            )
        return Batch.to_dict(batch)


class DataprocListBatchesOperator(GoogleCloudBaseOperator):
    """List batch workloads.

    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param project_id: Optional. The ID of the Google Cloud project that the cluster belongs to.
    :param page_size: Optional. The maximum number of batches to return in each response. The service may
        return fewer than this value. The default page size is 20; the maximum page size is 1000.
    :param page_token: Optional. A page token received from a previous ``ListBatches`` call.
        Provide this token to retrieve the subsequent page.
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :param metadata: Optional, additional metadata that is provided to the method.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param filter: Result filters as specified in ListBatchesRequest
    :param order_by: How to order results as specified in ListBatchesRequest
    """

    template_fields: Sequence[str] = ("region", "project_id", "impersonation_chain")
    operator_extra_links = (DataprocBatchesListLink(),)

    def __init__(
        self,
        *,
        region: str,
        project_id: str | None = None,
        page_size: int | None = None,
        page_token: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        filter: str | None = None,
        order_by: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.region = region
        self.project_id = project_id
        self.page_size = page_size
        self.page_token = page_token
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.filter = filter
        self.order_by = order_by

    def execute(self, context: Context):
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)
        results = hook.list_batches(
            region=self.region,
            project_id=self.project_id,
            page_size=self.page_size,
            page_token=self.page_token,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
            filter=self.filter,
            order_by=self.order_by,
        )
        project_id = self.project_id or hook.project_id
        if project_id:
            DataprocBatchesListLink.persist(context=context, operator=self, project_id=project_id)
        return [Batch.to_dict(result) for result in results]


class DataprocCancelOperationOperator(GoogleCloudBaseOperator):
    """Cancel the batch workload resource.

    :param operation_name: Required. The name of the operation resource to be cancelled.
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :param project_id: Optional. The ID of the Google Cloud project that the cluster belongs to.
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = ("operation_name", "region", "project_id", "impersonation_chain")

    def __init__(
        self,
        *,
        operation_name: str,
        region: str,
        project_id: str | None = None,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.operation_name = operation_name
        self.region = region
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain)

        self.log.info("Canceling operation: %s", self.operation_name)
        hook.get_operations_client(region=self.region).cancel_operation(name=self.operation_name)
        self.log.info("Operation canceled.")
