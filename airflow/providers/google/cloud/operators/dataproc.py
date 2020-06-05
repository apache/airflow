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
#
"""
This module contains Google Dataproc operators.
"""
# pylint: disable=C0302

import inspect
import ntpath
import os
import re
import time
import uuid
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Set, Tuple, Union

from google.api_core.exceptions import AlreadyExists
from google.api_core.retry import Retry
from google.cloud.dataproc_v1beta2.types import (  # pylint: disable=no-name-in-module
    Cluster, Duration, FieldMask,
)
from google.protobuf.json_format import MessageToDict

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook, DataProcJobBuilder
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from airflow.version import version as airflow_version


# pylint: disable=too-many-instance-attributes
class ClusterGenerator:
    """
    Create a new Dataproc Cluster.

    :param cluster_name: The name of the DataProc cluster to create. (templated)
    :type cluster_name: str
    :param project_id: The ID of the google cloud project in which
        to create the cluster. (templated)
    :type project_id: str
    :param num_workers: The # of workers to spin up. If set to zero will
        spin up cluster in a single node mode
    :type num_workers: int
    :param storage_bucket: The storage bucket to use, setting to None lets dataproc
        generate a custom one for you
    :type storage_bucket: str
    :param init_actions_uris: List of GCS uri's containing
        dataproc initialization scripts
    :type init_actions_uris: list[str]
    :param init_action_timeout: Amount of time executable scripts in
        init_actions_uris has to complete
    :type init_action_timeout: str
    :param metadata: dict of key-value google compute engine metadata entries
        to add to all instances
    :type metadata: dict
    :param image_version: the version of software inside the Dataproc cluster
    :type image_version: str
    :param custom_image: custom Dataproc image for more info see
        https://cloud.google.com/dataproc/docs/guides/dataproc-images
    :type custom_image: str
    :param custom_image_project_id: project id for the custom Dataproc image, for more info see
        https://cloud.google.com/dataproc/docs/guides/dataproc-images
    :type custom_image_project_id: str
    :param autoscaling_policy: The autoscaling policy used by the cluster. Only resource names
        including projectid and location (region) are valid. Example:
        ``projects/[projectId]/locations/[dataproc_region]/autoscalingPolicies/[policy_id]``
    :type autoscaling_policy: str
    :param properties: dict of properties to set on
        config files (e.g. spark-defaults.conf), see
        https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters#SoftwareConfig
    :type properties: dict
    :param optional_components: List of optional cluster components, for more info see
        https://cloud.google.com/dataproc/docs/reference/rest/v1/ClusterConfig#Component
    :type optional_components: list[str]
    :param num_masters: The # of master nodes to spin up
    :type num_masters: int
    :param master_machine_type: Compute engine machine type to use for the master node
    :type master_machine_type: str
    :param master_disk_type: Type of the boot disk for the master node
        (default is ``pd-standard``).
        Valid values: ``pd-ssd`` (Persistent Disk Solid State Drive) or
        ``pd-standard`` (Persistent Disk Hard Disk Drive).
    :type master_disk_type: str
    :param master_disk_size: Disk size for the master node
    :type master_disk_size: int
    :param worker_machine_type: Compute engine machine type to use for the worker nodes
    :type worker_machine_type: str
    :param worker_disk_type: Type of the boot disk for the worker node
        (default is ``pd-standard``).
        Valid values: ``pd-ssd`` (Persistent Disk Solid State Drive) or
        ``pd-standard`` (Persistent Disk Hard Disk Drive).
    :type worker_disk_type: str
    :param worker_disk_size: Disk size for the worker nodes
    :type worker_disk_size: int
    :param num_preemptible_workers: The # of preemptible worker nodes to spin up
    :type num_preemptible_workers: int
    :param labels: dict of labels to add to the cluster
    :type labels: dict
    :param zone: The zone where the cluster will be located. Set to None to auto-zone. (templated)
    :type zone: str
    :param network_uri: The network uri to be used for machine communication, cannot be
        specified with subnetwork_uri
    :type network_uri: str
    :param subnetwork_uri: The subnetwork uri to be used for machine communication,
        cannot be specified with network_uri
    :type subnetwork_uri: str
    :param internal_ip_only: If true, all instances in the cluster will only
        have internal IP addresses. This can only be enabled for subnetwork
        enabled networks
    :type internal_ip_only: bool
    :param tags: The GCE tags to add to all instances
    :type tags: list[str]
    :param region: leave as 'global', might become relevant in the future. (templated)
    :type region: str
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param service_account: The service account of the dataproc instances.
    :type service_account: str
    :param service_account_scopes: The URIs of service account scopes to be included.
    :type service_account_scopes: list[str]
    :param idle_delete_ttl: The longest duration that cluster would keep alive while
        staying idle. Passing this threshold will cause cluster to be auto-deleted.
        A duration in seconds.
    :type idle_delete_ttl: int
    :param auto_delete_time:  The time when cluster will be auto-deleted.
    :type auto_delete_time: datetime.datetime
    :param auto_delete_ttl: The life duration of cluster, the cluster will be
        auto-deleted at the end of this duration.
        A duration in seconds. (If auto_delete_time is set this parameter will be ignored)
    :type auto_delete_ttl: int
    :param customer_managed_key: The customer-managed key used for disk encryption
        ``projects/[PROJECT_STORING_KEYS]/locations/[LOCATION]/keyRings/[KEY_RING_NAME]/cryptoKeys/[KEY_NAME]`` # noqa # pylint: disable=line-too-long
    :type customer_managed_key: str
    """
    # pylint: disable=too-many-arguments,too-many-locals
    def __init__(self,
                 project_id: Optional[str] = None,
                 cluster_name: Optional[str] = None,
                 num_workers: Optional[int] = None,
                 zone: Optional[str] = None,
                 network_uri: Optional[str] = None,
                 subnetwork_uri: Optional[str] = None,
                 internal_ip_only: Optional[bool] = None,
                 tags: Optional[List[str]] = None,
                 storage_bucket: Optional[str] = None,
                 init_actions_uris: Optional[List[str]] = None,
                 init_action_timeout: str = "10m",
                 metadata: Optional[Dict] = None,
                 custom_image: Optional[str] = None,
                 custom_image_project_id: Optional[str] = None,
                 image_version: Optional[str] = None,
                 autoscaling_policy: Optional[str] = None,
                 properties: Optional[Dict] = None,
                 optional_components: Optional[List[str]] = None,
                 num_masters: int = 1,
                 master_machine_type: str = 'n1-standard-4',
                 master_disk_type: str = 'pd-standard',
                 master_disk_size: int = 1024,
                 worker_machine_type: str = 'n1-standard-4',
                 worker_disk_type: str = 'pd-standard',
                 worker_disk_size: int = 1024,
                 num_preemptible_workers: int = 0,
                 labels: Optional[Dict] = None,
                 region: Optional[str] = None,
                 service_account: Optional[str] = None,
                 service_account_scopes: Optional[List[str]] = None,
                 idle_delete_ttl: Optional[int] = None,
                 auto_delete_time: Optional[datetime] = None,
                 auto_delete_ttl: Optional[int] = None,
                 customer_managed_key: Optional[str] = None,
                 *args,  # just in case
                 **kwargs
                 ) -> None:

        self.cluster_name = cluster_name
        self.project_id = project_id
        self.region = region
        self.num_masters = num_masters
        self.num_workers = num_workers
        self.num_preemptible_workers = num_preemptible_workers
        self.storage_bucket = storage_bucket
        self.init_actions_uris = init_actions_uris
        self.init_action_timeout = init_action_timeout
        self.metadata = metadata
        self.custom_image = custom_image
        self.custom_image_project_id = custom_image_project_id
        self.image_version = image_version
        self.properties = properties or dict()
        self.optional_components = optional_components
        self.master_machine_type = master_machine_type
        self.master_disk_type = master_disk_type
        self.master_disk_size = master_disk_size
        self.autoscaling_policy = autoscaling_policy
        self.worker_machine_type = worker_machine_type
        self.worker_disk_type = worker_disk_type
        self.worker_disk_size = worker_disk_size
        self.labels = labels
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
        self.single_node = num_workers == 0

        if self.custom_image and self.image_version:
            raise ValueError("The custom_image and image_version can't be both set")

        if self.single_node and self.num_preemptible_workers > 0:
            raise ValueError("Single node cannot have preemptible workers.")

    def _get_init_action_timeout(self):
        match = re.match(r"^(\d+)([sm])$", self.init_action_timeout)
        if match:
            if match.group(2) == "s":
                return self.init_action_timeout
            elif match.group(2) == "m":
                val = float(match.group(1))
                return "{}s".format(int(timedelta(minutes=val).total_seconds()))

        raise AirflowException(
            "DataprocClusterCreateOperator init_action_timeout"
            " should be expressed in minutes or seconds. i.e. 10m, 30s")

    def _build_gce_cluster_config(self, cluster_data):
        if self.zone:
            zone_uri = \
                'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(
                    self.project_id, self.zone
                )
            cluster_data['config']['gce_cluster_config']['zone_uri'] = zone_uri

        if self.metadata:
            cluster_data['config']['gce_cluster_config']['metadata'] = self.metadata

        if self.network_uri:
            cluster_data['config']['gce_cluster_config']['network_uri'] = self.network_uri

        if self.subnetwork_uri:
            cluster_data['config']['gce_cluster_config']['subnetwork_uri'] = \
                self.subnetwork_uri

        if self.internal_ip_only:
            if not self.subnetwork_uri:
                raise AirflowException("Set internal_ip_only to true only when"
                                       " you pass a subnetwork_uri.")
            cluster_data['config']['gce_cluster_config']['internal_ip_only'] = True

        if self.tags:
            cluster_data['config']['gce_cluster_config']['tags'] = self.tags

        if self.service_account:
            cluster_data['config']['gce_cluster_config']['service_account'] = \
                self.service_account

        if self.service_account_scopes:
            cluster_data['config']['gce_cluster_config']['service_account_scopes'] = \
                self.service_account_scopes

        return cluster_data

    def _build_lifecycle_config(self, cluster_data):
        if self.idle_delete_ttl:
            cluster_data['config']['lifecycle_config']['idle_delete_ttl'] = \
                "{}s".format(self.idle_delete_ttl)

        if self.auto_delete_time:
            utc_auto_delete_time = timezone.convert_to_utc(self.auto_delete_time)
            cluster_data['config']['lifecycle_config']['auto_delete_time'] = \
                utc_auto_delete_time.format('%Y-%m-%dT%H:%M:%S.%fZ', formatter='classic')
        elif self.auto_delete_ttl:
            cluster_data['config']['lifecycle_config']['auto_delete_ttl'] = \
                "{}s".format(self.auto_delete_ttl)

        return cluster_data

    def _build_cluster_data(self):
        if self.zone:
            master_type_uri = \
                "https://www.googleapis.com/compute/v1/projects/{}/zones/{}/machineTypes/{}".format(
                    self.project_id, self.zone, self.master_machine_type)
            worker_type_uri = \
                "https://www.googleapis.com/compute/v1/projects/{}/zones/{}/machineTypes/{}".format(
                    self.project_id, self.zone, self.worker_machine_type)
        else:
            master_type_uri = self.master_machine_type
            worker_type_uri = self.worker_machine_type

        cluster_data = {
            'project_id': self.project_id,
            'cluster_name': self.cluster_name,
            'config': {
                'gce_cluster_config': {
                },
                'master_config': {
                    'num_instances': self.num_masters,
                    'machine_type_uri': master_type_uri,
                    'disk_config': {
                        'boot_disk_type': self.master_disk_type,
                        'boot_disk_size_gb': self.master_disk_size
                    }
                },
                'worker_config': {
                    'num_instances': self.num_workers,
                    'machine_type_uri': worker_type_uri,
                    'disk_config': {
                        'boot_disk_type': self.worker_disk_type,
                        'boot_disk_size_gb': self.worker_disk_size
                    }
                },
                'secondary_worker_config': {},
                'software_config': {},
                'lifecycle_config': {},
                'encryption_config': {},
                'autoscaling_config': {},
            }
        }
        if self.num_preemptible_workers > 0:
            cluster_data['config']['secondary_worker_config'] = {
                'num_instances': self.num_preemptible_workers,
                'machine_type_uri': worker_type_uri,
                'disk_config': {
                    'boot_disk_type': self.worker_disk_type,
                    'boot_disk_size_gb': self.worker_disk_size
                },
                'is_preemptible': True
            }

        cluster_data['labels'] = self.labels or {}

        # Dataproc labels must conform to the following regex:
        # [a-z]([-a-z0-9]*[a-z0-9])? (current airflow version string follows
        # semantic versioning spec: x.y.z).
        cluster_data['labels'].update({
            'airflow-version': 'v' + airflow_version.replace('.', '-').replace('+', '-')
        })
        if self.storage_bucket:
            cluster_data['config']['config_bucket'] = self.storage_bucket

        if self.image_version:
            cluster_data['config']['software_config']['image_version'] = self.image_version

        elif self.custom_image:
            project_id = self.custom_image_project_id or self.project_id
            custom_image_url = 'https://www.googleapis.com/compute/beta/projects/' \
                               '{}/global/images/{}'.format(project_id,
                                                            self.custom_image)
            cluster_data['config']['master_config']['image_uri'] = custom_image_url
            if not self.single_node:
                cluster_data['config']['worker_config']['image_uri'] = custom_image_url

        cluster_data = self._build_gce_cluster_config(cluster_data)

        if self.single_node:
            self.properties["dataproc:dataproc.allow.zero.workers"] = "true"

        if self.properties:
            cluster_data['config']['software_config']['properties'] = self.properties

        if self.optional_components:
            cluster_data['config']['software_config']['optional_components'] = self.optional_components

        cluster_data = self._build_lifecycle_config(cluster_data)

        if self.init_actions_uris:
            init_actions_dict = [
                {
                    'executable_file': uri,
                    'execution_timeout': self._get_init_action_timeout()
                } for uri in self.init_actions_uris
            ]
            cluster_data['config']['initialization_actions'] = init_actions_dict

        if self.customer_managed_key:
            cluster_data['config']['encryption_config'] = \
                {'gce_pd_kms_key_name': self.customer_managed_key}
        if self.autoscaling_policy:
            cluster_data['config']['autoscaling_config'] = {'policy_uri': self.autoscaling_policy}

        return cluster_data

    def make(self):
        """
        Helper method for easier migration.
        :return: Dict representing Dataproc cluster.
        """
        return self._build_cluster_data()


# pylint: disable=too-many-instance-attributes
class DataprocCreateClusterOperator(BaseOperator):
    """
    Create a new cluster on Google Cloud Dataproc. The operator will wait until the
    creation is successful or an error occurs in the creation process.

    The parameters allow to configure the cluster. Please refer to

    https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters

    for a detailed explanation on the different parameters. Most of the configuration
    parameters detailed in the link are available as a parameter to this operator.

    :param project_id: The ID of the google cloud project in which
        to create the cluster. (templated)
    :type project_id: str
    :param region: leave as 'global', might become relevant in the future. (templated)
    :type region: str
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``DeleteClusterRequest`` requests with the same id, then the second request will be ignored and the
        first ``google.longrunning.Operation`` created and stored in the backend is returned.
    :type request_id: str
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ('project_id', 'region', 'cluster')

    @apply_defaults
    def __init__(self,
                 region: str = 'global',
                 project_id: Optional[str] = None,
                 cluster: Optional[Dict] = None,
                 request_id: Optional[str] = None,
                 retry: Optional[Retry] = None,
                 timeout: Optional[float] = None,
                 metadata: Optional[Sequence[Tuple[str, str]]] = None,
                 gcp_conn_id: str = "google_cloud_default",
                 *args,
                 **kwargs) -> None:
        # TODO: remove one day
        if cluster is None:
            warnings.warn(
                "Passing cluster parameters by keywords to `{}` "
                "will be deprecated. Please provide cluster object using `cluster` parameter. "
                "You can use `airflow.dataproc.ClusterGenerator.generate_cluster` method to "
                "obtain cluster object.".format(type(self).__name__),
                DeprecationWarning, stacklevel=1
            )
            # Remove result of apply defaults
            if 'params' in kwargs:
                del kwargs['params']

            # Create cluster object from kwargs
            kwargs['region'] = region
            kwargs['project_id'] = project_id
            cluster = ClusterGenerator(**kwargs).make()

            # Remove from kwargs cluster params passed for backward compatibility
            cluster_params = inspect.signature(ClusterGenerator.__init__).parameters
            for arg in cluster_params:
                if arg in kwargs:
                    del kwargs[arg]

        super().__init__(*args, **kwargs)

        self.cluster = cluster
        self.cluster_name = cluster.get('cluster_name')
        self.project_id = project_id
        self.region = region
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        self.log.info('Creating cluster: %s', self.cluster_name)
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id)
        try:
            operation = hook.create_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster=self.cluster,
                request_id=self.request_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            cluster = operation.result()
            self.log.info("Cluster created.")
        except AlreadyExists:
            cluster = hook.get_cluster(
                project_id=self.project_id,
                region=self.region,
                cluster_name=self.cluster_name,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            self.log.info("Cluster already exists.")
        return MessageToDict(cluster)


class DataprocScaleClusterOperator(BaseOperator):
    """
    Scale, up or down, a cluster on Google Cloud Dataproc.
    The operator will wait until the cluster is re-scaled.

    **Example**: ::

        t1 = DataprocClusterScaleOperator(
                task_id='dataproc_scale',
                project_id='my-project',
                cluster_name='cluster-1',
                num_workers=10,
                num_preemptible_workers=10,
                graceful_decommission_timeout='1h',
                dag=dag)

    .. seealso::
        For more detail on about scaling clusters have a look at the reference:
        https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/scaling-clusters

    :param cluster_name: The name of the cluster to scale. (templated)
    :type cluster_name: str
    :param project_id: The ID of the google cloud project in which
        the cluster runs. (templated)
    :type project_id: str
    :param region: The region for the dataproc cluster. (templated)
    :type region: str
    :param num_workers: The new number of workers
    :type num_workers: int
    :param num_preemptible_workers: The new number of preemptible workers
    :type num_preemptible_workers: int
    :param graceful_decommission_timeout: Timeout for graceful YARN decomissioning.
        Maximum value is 1d
    :type graceful_decommission_timeout: str
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ['cluster_name', 'project_id', 'region']

    @apply_defaults
    def __init__(self,
                 cluster_name: str,
                 project_id: Optional[str] = None,
                 region: str = 'global',
                 num_workers: int = 2,
                 num_preemptible_workers: int = 0,
                 graceful_decommission_timeout: Optional[str] = None,
                 gcp_conn_id: str = "google_cloud_default",
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.num_workers = num_workers
        self.num_preemptible_workers = num_preemptible_workers
        self.graceful_decommission_timeout = graceful_decommission_timeout
        self.gcp_conn_id = gcp_conn_id

        # TODO: Remove one day
        warnings.warn(
            "The `{cls}` operator is deprecated, please use `DataprocUpdateClusterOperator` instead.".format(
                cls=type(self).__name__
            ),
            DeprecationWarning,
            stacklevel=1
        )

    def _build_scale_cluster_data(self):
        scale_data = {
            'config': {
                'worker_config': {
                    'num_instances': self.num_workers
                },
                'secondary_worker_config': {
                    'num_instances': self.num_preemptible_workers
                }
            }
        }
        return scale_data

    @property
    def _graceful_decommission_timeout_object(self) -> Optional[Dict]:
        if not self.graceful_decommission_timeout:
            return None

        timeout = None
        match = re.match(r"^(\d+)([smdh])$", self.graceful_decommission_timeout)
        if match:
            if match.group(2) == "s":
                timeout = int(match.group(1))
            elif match.group(2) == "m":
                val = float(match.group(1))
                timeout = int(timedelta(minutes=val).total_seconds())
            elif match.group(2) == "h":
                val = float(match.group(1))
                timeout = int(timedelta(hours=val).total_seconds())
            elif match.group(2) == "d":
                val = float(match.group(1))
                timeout = int(timedelta(days=val).total_seconds())

        if not timeout:
            raise AirflowException(
                "DataprocClusterScaleOperator "
                " should be expressed in day, hours, minutes or seconds. "
                " i.e. 1d, 4h, 10m, 30s"
            )

        return {'seconds': timeout}

    def execute(self, context):
        """
        Scale, up or down, a cluster on Google Cloud Dataproc.
        """
        self.log.info("Scaling cluster: %s", self.cluster_name)

        scaling_cluster_data = self._build_scale_cluster_data()
        update_mask = [
            "config.worker_config.num_instances",
            "config.secondary_worker_config.num_instances"
        ]

        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id)
        operation = hook.update_cluster(
            project_id=self.project_id,
            location=self.region,
            cluster_name=self.cluster_name,
            cluster=scaling_cluster_data,
            graceful_decommission_timeout=self._graceful_decommission_timeout_object,
            update_mask={'paths': update_mask},
        )
        operation.result()
        self.log.info("Cluster scaling finished")


class DataprocDeleteClusterOperator(BaseOperator):
    """
    Deletes a cluster in a project.

    :param project_id: Required. The ID of the Google Cloud Platform project that the cluster belongs to.
    :type project_id: str
    :param region: Required. The Cloud Dataproc region in which to handle the request.
    :type region: str
    :param cluster_name: Required. The cluster name.
    :type cluster_name: str
    :param cluster_uuid: Optional. Specifying the ``cluster_uuid`` means the RPC should fail
        if cluster with specified UUID does not exist.
    :type cluster_uuid: str
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``DeleteClusterRequest`` requests with the same id, then the second request will be ignored and the
        first ``google.longrunning.Operation`` created and stored in the backend is returned.
    :type request_id: str
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        project_id: str,
        region: str,
        cluster_name: str,
        cluster_uuid: Optional[str] = None,
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name
        self.cluster_uuid = cluster_uuid
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id)
        self.log.info("Deleting cluster: %s", self.cluster_name)
        operation = hook.delete_cluster(
            project_id=self.project_id,
            region=self.region,
            cluster_name=self.cluster_name,
            cluster_uuid=self.cluster_uuid,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        operation.result()
        self.log.info("Cluster deleted.")


class DataprocJobBaseOperator(BaseOperator):
    """
    The base class for operators that launch job on DataProc.

    :param job_name: The job name used in the DataProc cluster. This name by default
        is the task_id appended with the execution data, but can be templated. The
        name will always be appended with a random number to avoid name clashes.
    :type job_name: str
    :param cluster_name: The name of the DataProc cluster.
    :type cluster_name: str
    :param dataproc_properties: Map for the Hive properties. Ideal to put in
        default arguments (templated)
    :type dataproc_properties: dict
    :param dataproc_jars: HCFS URIs of jar files to add to the CLASSPATH of the Hive server and Hadoop
        MapReduce (MR) tasks. Can contain Hive SerDes and UDFs. (templated)
    :type dataproc_jars: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param labels: The labels to associate with this job. Label keys must contain 1 to 63 characters,
        and must conform to RFC 1035. Label values may be empty, but, if present, must contain 1 to 63
        characters, and must conform to RFC 1035. No more than 32 labels can be associated with a job.
    :type labels: dict
    :param region: The specified region where the dataproc cluster is created.
    :type region: str
    :param job_error_states: Job states that should be considered error states.
        Any states in this set will result in an error being raised and failure of the
        task. Eg, if the ``CANCELLED`` state should also be considered a task failure,
        pass in ``{'ERROR', 'CANCELLED'}``. Possible values are currently only
        ``'ERROR'`` and ``'CANCELLED'``, but could change in the future. Defaults to
        ``{'ERROR'}``.
    :type job_error_states: set
    :var dataproc_job_id: The actual "jobId" as submitted to the Dataproc API.
        This is useful for identifying or linking to the job in the Google Cloud Console
        Dataproc UI, as the actual "jobId" submitted to the Dataproc API is appended with
        an 8 character random string.
    :vartype dataproc_job_id: str
    """
    job_type = ""

    @apply_defaults
    def __init__(self,
                 job_name: str = '{{task.task_id}}_{{ds_nodash}}',
                 cluster_name: str = "cluster-1",
                 dataproc_properties: Optional[Dict] = None,
                 dataproc_jars: Optional[List[str]] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 labels: Optional[Dict] = None,
                 region: str = 'global',
                 job_error_states: Optional[Set[str]] = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.labels = labels
        self.job_name = job_name
        self.cluster_name = cluster_name
        self.dataproc_properties = dataproc_properties
        self.dataproc_jars = dataproc_jars
        self.region = region
        self.job_error_states = job_error_states if job_error_states is not None else {'ERROR'}

        self.hook = DataprocHook(gcp_conn_id=gcp_conn_id)
        self.project_id = self.hook.project_id
        self.job_template = None
        self.job = None
        self.dataproc_job_id = None

    def create_job_template(self):
        """
        Initialize `self.job_template` with default values
        """
        self.job_template = DataProcJobBuilder(
            project_id=self.project_id,
            task_id=self.task_id,
            cluster_name=self.cluster_name,
            job_type=self.job_type,
            properties=self.dataproc_properties
        )
        self.job_template.set_job_name(self.job_name)
        self.job_template.add_jar_file_uris(self.dataproc_jars)
        self.job_template.add_labels(self.labels)

    def _generate_job_template(self):
        if self.job_template:
            job = self.job_template.build()
            return job['job']
        raise Exception("Create a job template before")

    def execute(self, context):
        if self.job_template:
            self.job = self.job_template.build()
            self.dataproc_job_id = self.job["job"]["reference"]["job_id"]
            self.log.info('Submitting %s job %s', self.job_type, self.dataproc_job_id)
            job_object = self.hook.submit_job(
                project_id=self.project_id,
                job=self.job["job"],
                location=self.region,
            )
            job_id = job_object.reference.job_id
            self.hook.wait_for_job(
                job_id=job_id,
                location=self.region,
                project_id=self.project_id
            )
            self.log.info('Job executed correctly.')
        else:
            raise AirflowException("Create a job template before")

    def on_kill(self):
        """
        Callback called when the operator is killed.
        Cancel any running job.
        """
        if self.dataproc_job_id:
            self.hook.cancel_job(
                project_id=self.project_id,
                job_id=self.dataproc_job_id,
                location=self.region
            )


class DataprocSubmitPigJobOperator(DataprocJobBaseOperator):
    """
    Start a Pig query Job on a Cloud DataProc cluster. The parameters of the operation
    will be passed to the cluster.

    It's a good practice to define dataproc_* parameters in the default_args of the dag
    like the cluster name and UDFs.

    .. code-block:: python

        default_args = {
            'cluster_name': 'cluster-1',
            'dataproc_pig_jars': [
                'gs://example/udf/jar/datafu/1.2.0/datafu.jar',
                'gs://example/udf/jar/gpig/1.2/gpig.jar'
            ]
        }

    You can pass a pig script as string or file reference. Use variables to pass on
    variables for the pig script to be resolved on the cluster or use the parameters to
    be resolved in the script as template parameters.

    **Example**: ::

        t1 = DataProcPigOperator(
                task_id='dataproc_pig',
                query='a_pig_script.pig',
                variables={'out': 'gs://example/output/{{ds}}'},
                dag=dag)

    .. seealso::
        For more detail on about job submission have a look at the reference:
        https://cloud.google.com/dataproc/reference/rest/v1/projects.regions.jobs

    :param query: The query or reference to the query
        file (pg or pig extension). (templated)
    :type query: str
    :param query_uri: The HCFS URI of the script that contains the Pig queries.
    :type query_uri: str
    :param variables: Map of named parameters for the query. (templated)
    :type variables: dict
    """
    template_fields = ['query', 'variables', 'job_name', 'cluster_name',
                       'region', 'dataproc_jars', 'dataproc_properties']
    template_ext = ('.pg', '.pig',)
    ui_color = '#0273d4'
    job_type = 'pig_job'

    @apply_defaults
    def __init__(
        self,
        query: Optional[str] = None,
        query_uri: Optional[str] = None,
        variables: Optional[Dict] = None,
        *args,
        **kwargs
    ) -> None:
        # TODO: Remove one day
        warnings.warn(
            "The `{cls}` operator is deprecated, please use `DataprocSubmitJobOperator` instead. You can use"
            " `generate_job` method of `{cls}` to generate dictionary representing your job"
            " and use it with the new operator.".format(cls=type(self).__name__),
            DeprecationWarning,
            stacklevel=1
        )

        super().__init__(*args, **kwargs)
        self.query = query
        self.query_uri = query_uri
        self.variables = variables

    def generate_job(self):
        """
        Helper method for easier migration to `DataprocSubmitJobOperator`.
        :return: Dict representing Dataproc job
        """
        self.create_job_template()

        if self.query is None:
            self.job_template.add_query_uri(self.query_uri)
        else:
            self.job_template.add_query(self.query)
        self.job_template.add_variables(self.variables)
        return self._generate_job_template()

    def execute(self, context):
        self.create_job_template()

        if self.query is None:
            self.job_template.add_query_uri(self.query_uri)
        else:
            self.job_template.add_query(self.query)
        self.job_template.add_variables(self.variables)

        super().execute(context)


class DataprocSubmitHiveJobOperator(DataprocJobBaseOperator):
    """
    Start a Hive query Job on a Cloud DataProc cluster.

    :param query: The query or reference to the query file (q extension).
    :type query: str
    :param query_uri: The HCFS URI of the script that contains the Hive queries.
    :type query_uri: str
    :param variables: Map of named parameters for the query.
    :type variables: dict
    """
    template_fields = ['query', 'variables', 'job_name', 'cluster_name',
                       'region', 'dataproc_jars', 'dataproc_properties']
    template_ext = ('.q', '.hql',)
    ui_color = '#0273d4'
    job_type = 'hive_job'

    @apply_defaults
    def __init__(
        self,
        query: Optional[str] = None,
        query_uri: Optional[str] = None,
        variables: Optional[Dict] = None,
        *args,
        **kwargs
    ) -> None:
        # TODO: Remove one day
        warnings.warn(
            "The `{cls}` operator is deprecated, please use `DataprocSubmitJobOperator` instead. You can use"
            " `generate_job` method of `{cls}` to generate dictionary representing your job"
            " and use it with the new operator.".format(cls=type(self).__name__),
            DeprecationWarning,
            stacklevel=1
        )

        super().__init__(*args, **kwargs)
        self.query = query
        self.query_uri = query_uri
        self.variables = variables
        if self.query is not None and self.query_uri is not None:
            raise AirflowException('Only one of `query` and `query_uri` can be passed.')

    def generate_job(self):
        """
        Helper method for easier migration to `DataprocSubmitJobOperator`.
        :return: Dict representing Dataproc job
        """
        self.create_job_template()
        if self.query is None:
            self.job_template.add_query_uri(self.query_uri)
        else:
            self.job_template.add_query(self.query)
        self.job_template.add_variables(self.variables)
        return self._generate_job_template()

    def execute(self, context):
        self.create_job_template()
        if self.query is None:
            self.job_template.add_query_uri(self.query_uri)
        else:
            self.job_template.add_query(self.query)
        self.job_template.add_variables(self.variables)

        super().execute(context)


class DataprocSubmitSparkSqlJobOperator(DataprocJobBaseOperator):
    """
    Start a Spark SQL query Job on a Cloud DataProc cluster.

    :param query: The query or reference to the query file (q extension). (templated)
    :type query: str
    :param query_uri: The HCFS URI of the script that contains the SQL queries.
    :type query_uri: str
    :param variables: Map of named parameters for the query. (templated)
    :type variables: dict
    """
    template_fields = ['query', 'variables', 'job_name', 'cluster_name',
                       'region', 'dataproc_jars', 'dataproc_properties']
    template_ext = ('.q',)
    ui_color = '#0273d4'
    job_type = 'spark_sql_job'

    @apply_defaults
    def __init__(
        self,
        query: Optional[str] = None,
        query_uri: Optional[str] = None,
        variables: Optional[Dict] = None,
        *args,
        **kwargs
    ) -> None:
        # TODO: Remove one day
        warnings.warn(
            "The `{cls}` operator is deprecated, please use `DataprocSubmitJobOperator` instead. You can use"
            " `generate_job` method of `{cls}` to generate dictionary representing your job"
            " and use it with the new operator.".format(cls=type(self).__name__),
            DeprecationWarning,
            stacklevel=1
        )

        super().__init__(*args, **kwargs)
        self.query = query
        self.query_uri = query_uri
        self.variables = variables
        if self.query is not None and self.query_uri is not None:
            raise AirflowException('Only one of `query` and `query_uri` can be passed.')

    def generate_job(self):
        """
        Helper method for easier migration to `DataprocSubmitJobOperator`.
        :return: Dict representing Dataproc job
        """
        self.create_job_template()
        if self.query is None:
            self.job_template.add_query_uri(self.query_uri)
        else:
            self.job_template.add_query(self.query)
        self.job_template.add_variables(self.variables)
        return self._generate_job_template()

    def execute(self, context):
        self.create_job_template()
        if self.query is None:
            self.job_template.add_query_uri(self.query_uri)
        else:
            self.job_template.add_query(self.query)
        self.job_template.add_variables(self.variables)

        super().execute(context)


class DataprocSubmitSparkJobOperator(DataprocJobBaseOperator):
    """
    Start a Spark Job on a Cloud DataProc cluster.

    :param main_jar: The HCFS URI of the jar file that contains the main class
        (use this or the main_class, not both together).
    :type main_jar: str
    :param main_class: Name of the job class. (use this or the main_jar, not both
        together).
    :type main_class: str
    :param arguments: Arguments for the job. (templated)
    :type arguments: list
    :param archives: List of archived files that will be unpacked in the work
        directory. Should be stored in Cloud Storage.
    :type archives: list
    :param files: List of files to be copied to the working directory
    :type files: list
    """

    template_fields = ['arguments', 'job_name', 'cluster_name',
                       'region', 'dataproc_jars', 'dataproc_properties']
    ui_color = '#0273d4'
    job_type = 'spark_job'

    @apply_defaults
    def __init__(
        self,
        main_jar: Optional[str] = None,
        main_class: Optional[str] = None,
        arguments: Optional[List] = None,
        archives: Optional[List] = None,
        files: Optional[List] = None,
        *args,
        **kwargs
    ) -> None:
        # TODO: Remove one day
        warnings.warn(
            "The `{cls}` operator is deprecated, please use `DataprocSubmitJobOperator` instead. You can use"
            " `generate_job` method of `{cls}` to generate dictionary representing your job"
            " and use it with the new operator.".format(cls=type(self).__name__),
            DeprecationWarning,
            stacklevel=1
        )

        super().__init__(*args, **kwargs)
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
        self.create_job_template()
        self.job_template.set_main(self.main_jar, self.main_class)
        self.job_template.add_args(self.arguments)
        self.job_template.add_archive_uris(self.archives)
        self.job_template.add_file_uris(self.files)
        return self._generate_job_template()

    def execute(self, context):
        self.create_job_template()
        self.job_template.set_main(self.main_jar, self.main_class)
        self.job_template.add_args(self.arguments)
        self.job_template.add_archive_uris(self.archives)
        self.job_template.add_file_uris(self.files)

        super().execute(context)


class DataprocSubmitHadoopJobOperator(DataprocJobBaseOperator):
    """
    Start a Hadoop Job on a Cloud DataProc cluster.

    :param main_jar: The HCFS URI of the jar file containing the main class
        (use this or the main_class, not both together).
    :type main_jar: str
    :param main_class: Name of the job class. (use this or the main_jar, not both
        together).
    :type main_class: str
    :param arguments: Arguments for the job. (templated)
    :type arguments: list
    :param archives: List of archived files that will be unpacked in the work
        directory. Should be stored in Cloud Storage.
    :type archives: list
    :param files: List of files to be copied to the working directory
    :type files: list
    """

    template_fields = ['arguments', 'job_name', 'cluster_name',
                       'region', 'dataproc_jars', 'dataproc_properties']
    ui_color = '#0273d4'
    job_type = 'hadoop_job'

    @apply_defaults
    def __init__(
        self,
        main_jar: Optional[str] = None,
        main_class: Optional[str] = None,
        arguments: Optional[List] = None,
        archives: Optional[List] = None,
        files: Optional[List] = None,
        *args,
        **kwargs
    ) -> None:
        # TODO: Remove one day
        warnings.warn(
            "The `{cls}` operator is deprecated, please use `DataprocSubmitJobOperator` instead. You can use"
            " `generate_job` method of `{cls}` to generate dictionary representing your job"
            " and use it with the new operator.".format(cls=type(self).__name__),
            DeprecationWarning,
            stacklevel=1
        )

        super().__init__(*args, **kwargs)
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
        self.create_job_template()
        self.job_template.set_main(self.main_jar, self.main_class)
        self.job_template.add_args(self.arguments)
        self.job_template.add_archive_uris(self.archives)
        self.job_template.add_file_uris(self.files)
        return self._generate_job_template()

    def execute(self, context):
        self.create_job_template()
        self.job_template.set_main(self.main_jar, self.main_class)
        self.job_template.add_args(self.arguments)
        self.job_template.add_archive_uris(self.archives)
        self.job_template.add_file_uris(self.files)

        super().execute(context)


class DataprocSubmitPySparkJobOperator(DataprocJobBaseOperator):
    """
    Start a PySpark Job on a Cloud DataProc cluster.

    :param main: [Required] The Hadoop Compatible Filesystem (HCFS) URI of the main
            Python file to use as the driver. Must be a .py file. (templated)
    :type main: str
    :param arguments: Arguments for the job. (templated)
    :type arguments: list
    :param archives: List of archived files that will be unpacked in the work
        directory. Should be stored in Cloud Storage.
    :type archives: list
    :param files: List of files to be copied to the working directory
    :type files: list
    :param pyfiles: List of Python files to pass to the PySpark framework.
        Supported file types: .py, .egg, and .zip
    :type pyfiles: list
    """

    template_fields = ['main', 'arguments', 'job_name', 'cluster_name',
                       'region', 'dataproc_jars', 'dataproc_properties']
    ui_color = '#0273d4'
    job_type = 'pyspark_job'

    @staticmethod
    def _generate_temp_filename(filename):
        date = time.strftime('%Y%m%d%H%M%S')
        return "{}_{}_{}".format(date, str(uuid.uuid4())[:8], ntpath.basename(filename))

    def _upload_file_temp(self, bucket, local_file):
        """
        Upload a local file to a Google Cloud Storage bucket.
        """
        temp_filename = self._generate_temp_filename(local_file)
        if not bucket:
            raise AirflowException(
                "If you want Airflow to upload the local file to a temporary bucket, set "
                "the 'temp_bucket' key in the connection string")

        self.log.info("Uploading %s to %s", local_file, temp_filename)

        GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id
        ).upload(
            bucket_name=bucket,
            object_name=temp_filename,
            mime_type='application/x-python',
            filename=local_file
        )
        return "gs://{}/{}".format(bucket, temp_filename)

    @apply_defaults
    def __init__(
        self,
        main: str,
        arguments: Optional[List] = None,
        archives: Optional[List] = None,
        pyfiles: Optional[List] = None,
        files: Optional[List] = None,
        *args,
        **kwargs
    ) -> None:
        # TODO: Remove one day
        warnings.warn(
            "The `{cls}` operator is deprecated, please use `DataprocSubmitJobOperator` instead. You can use"
            " `generate_job` method of `{cls}` to generate dictionary representing your job"
            " and use it with the new operator.".format(cls=type(self).__name__),
            DeprecationWarning,
            stacklevel=1
        )

        super().__init__(*args, **kwargs)
        self.main = main
        self.arguments = arguments
        self.archives = archives
        self.files = files
        self.pyfiles = pyfiles

    def generate_job(self):
        """
        Helper method for easier migration to `DataprocSubmitJobOperator`.
        :return: Dict representing Dataproc job
        """
        self.create_job_template()
        #  Check if the file is local, if that is the case, upload it to a bucket
        if os.path.isfile(self.main):
            cluster_info = self.hook.get_cluster(
                project_id=self.hook.project_id,
                region=self.region,
                cluster_name=self.cluster_name
            )
            bucket = cluster_info['config']['config_bucket']
            self.main = "gs://{}/{}".format(bucket, self.main)
        self.job_template.set_python_main(self.main)
        self.job_template.add_args(self.arguments)
        self.job_template.add_archive_uris(self.archives)
        self.job_template.add_file_uris(self.files)
        self.job_template.add_python_file_uris(self.pyfiles)

        return self._generate_job_template()

    def execute(self, context):
        self.create_job_template()
        #  Check if the file is local, if that is the case, upload it to a bucket
        if os.path.isfile(self.main):
            cluster_info = self.hook.get_cluster(
                project_id=self.hook.project_id,
                region=self.region,
                cluster_name=self.cluster_name
            )
            bucket = cluster_info['config']['config_bucket']
            self.main = self._upload_file_temp(bucket, self.main)

        self.job_template.set_python_main(self.main)
        self.job_template.add_args(self.arguments)
        self.job_template.add_archive_uris(self.archives)
        self.job_template.add_file_uris(self.files)
        self.job_template.add_python_file_uris(self.pyfiles)

        super().execute(context)


class DataprocInstantiateWorkflowTemplateOperator(BaseOperator):
    """
    Instantiate a WorkflowTemplate on Google Cloud Dataproc. The operator will wait
    until the WorkflowTemplate is finished executing.

    .. seealso::
        Please refer to:
        https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/projects.regions.workflowTemplates/instantiate

    :param template_id: The id of the template. (templated)
    :type template_id: str
    :param project_id: The ID of the google cloud project in which
        the template runs
    :type project_id: str
    :param region: leave as 'global', might become relevant in the future
    :type region: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param parameters: a map of parameters for Dataproc Template in key-value format:
        map (key: string, value: string)
        Example: { "date_from": "2019-08-01", "date_to": "2019-08-02"}.
        Values may not exceed 100 characters. Please refer to:
        https://cloud.google.com/dataproc/docs/concepts/workflows/workflow-parameters
    :type parameters: Dict[str, str]
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``SubmitJobRequest`` requests with the same id, then the second request will be ignored and the first
        ``Job`` created and stored in the backend is returned.
        It is recommended to always set this value to a UUID.
    :type request_id: str
    :param parameters: Optional. Map from parameter names to values that should be used for those
        parameters. Values may not exceed 100 characters.
    :type parameters: Dict[str, str]
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ['template_id']

    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
        self,
        template_id: str,
        region: str,
        project_id: Optional[str] = None,
        version: Optional[int] = None,
        request_id: Optional[str] = None,
        parameters: Optional[Dict[str, str]] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

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

    def execute(self, context):
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id)
        self.log.info('Instantiating template %s', self.template_id)
        operation = hook.instantiate_workflow_template(
            project_id=self.project_id,
            location=self.region,
            template_name=self.template_id,
            version=self.version,
            request_id=self.request_id,
            parameters=self.parameters,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        operation.result()
        self.log.info('Template instantiated.')


class DataprocInstantiateInlineWorkflowTemplateOperator(BaseOperator):
    """
    Instantiate a WorkflowTemplate Inline on Google Cloud Dataproc. The operator will
    wait until the WorkflowTemplate is finished executing.

    .. seealso::
        Please refer to:
        https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/projects.regions.workflowTemplates/instantiateInline

    :param template: The template contents. (templated)
    :type template: dict
    :param project_id: The ID of the google cloud project in which
        the template runs
    :type project_id: str
    :param region: leave as 'global', might become relevant in the future
    :type region: str
    :param parameters: a map of parameters for Dataproc Template in key-value format:
        map (key: string, value: string)
        Example: { "date_from": "2019-08-01", "date_to": "2019-08-02"}.
        Values may not exceed 100 characters. Please refer to:
        https://cloud.google.com/dataproc/docs/concepts/workflows/workflow-parameters
    :type parameters: Dict[str, str]
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``SubmitJobRequest`` requests with the same id, then the second request will be ignored and the first
        ``Job`` created and stored in the backend is returned.
        It is recommended to always set this value to a UUID.
    :type request_id: str
    :param parameters: Optional. Map from parameter names to values that should be used for those
        parameters. Values may not exceed 100 characters.
    :type parameters: Dict[str, str]
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ['template']

    @apply_defaults
    def __init__(
        self,
        template: Dict,
        region: str,
        project_id: Optional[str] = None,
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.template = template
        self.project_id = project_id
        self.location = region
        self.template = template
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        self.log.info('Instantiating Inline Template')
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id)
        operation = hook.instantiate_inline_workflow_template(
            template=self.template,
            project_id=self.project_id,
            location=self.location,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        operation.result()
        self.log.info('Template instantiated.')


class DataprocSubmitJobOperator(BaseOperator):
    """
    Submits a job to a cluster.

    :param project_id: Required. The ID of the Google Cloud Platform project that the job belongs to.
    :type project_id: str
    :param location: Required. The Cloud Dataproc region in which to handle the request.
    :type location: str
    :param job: Required. The job resource.
        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.dataproc_v1beta2.types.Job`
    :type job: Dict
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``SubmitJobRequest`` requests with the same id, then the second request will be ignored and the first
        ``Job`` created and stored in the backend is returned.
        It is recommended to always set this value to a UUID.
    :type request_id: str
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    template_fields = ('project_id', 'location', 'job')

    @apply_defaults
    def __init__(
        self,
        project_id: str,
        location: str,
        job: Dict,
        request_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.location = location
        self.job = job
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        self.log.info("Submitting job")
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id)
        job_object = hook.submit_job(
            project_id=self.project_id,
            location=self.location,
            job=self.job,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        job_id = job_object.reference.job_id
        self.log.info("Waiting for job %s to complete", job_id)
        hook.wait_for_job(
            job_id=job_id,
            project_id=self.project_id,
            location=self.location
        )
        self.log.info("Job completed successfully.")


class DataprocUpdateClusterOperator(BaseOperator):
    """
    Updates a cluster in a project.

    :param project_id: Required. The ID of the Google Cloud Platform project the cluster belongs to.
    :type project_id: str
    :param location: Required. The Cloud Dataproc region in which to handle the request.
    :type location: str
    :param cluster_name: Required. The cluster name.
    :type cluster_name: str
    :param cluster: Required. The changes to the cluster.

        If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.dataproc_v1beta2.types.Cluster`
    :type cluster: Union[Dict, google.cloud.dataproc_v1beta2.types.Cluster]
    :param update_mask: Required. Specifies the path, relative to ``Cluster``, of the field to update. For
        example, to change the number of workers in a cluster to 5, the ``update_mask`` parameter would be
        specified as ``config.worker_config.num_instances``, and the ``PATCH`` request body would specify the
        new value. If a dict is provided, it must be of the same form as the protobuf message
        :class:`~google.cloud.dataproc_v1beta2.types.FieldMask`
    :type update_mask: Union[Dict, google.cloud.dataproc_v1beta2.types.FieldMask]
    :param graceful_decommission_timeout: Optional. Timeout for graceful YARN decomissioning. Graceful
        decommissioning allows removing nodes from the cluster without interrupting jobs in progress. Timeout
        specifies how long to wait for jobs in progress to finish before forcefully removing nodes (and
        potentially interrupting jobs). Default timeout is 0 (for forceful decommission), and the maximum
        allowed timeout is 1 day.
    :type graceful_decommission_timeout: Union[Dict, google.cloud.dataproc_v1beta2.types.Duration]
    :param request_id: Optional. A unique id used to identify the request. If the server receives two
        ``UpdateClusterRequest`` requests with the same id, then the second request will be ignored and the
        first ``google.longrunning.Operation`` created and stored in the backend is returned.
    :type request_id: str
    :param retry: A retry object used to retry requests. If ``None`` is specified, requests will not be
        retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The amount of time, in seconds, to wait for the request to complete. Note that if
        ``retry`` is specified, the timeout applies to each individual attempt.
    :type timeout: float
    :param metadata: Additional metadata that is provided to the method.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(  # pylint: disable=too-many-arguments
        self,
        location: str,
        cluster_name: str,
        cluster: Union[Dict, Cluster],
        update_mask: Union[Dict, FieldMask],
        graceful_decommission_timeout: Union[Dict, Duration],
        request_id: Optional[str] = None,
        project_id: Optional[str] = None,
        retry: Retry = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.cluster = cluster
        self.update_mask = update_mask
        self.graceful_decommission_timeout = graceful_decommission_timeout
        self.request_id = request_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id)
        self.log.info("Updating %s cluster.", self.cluster_name)
        operation = hook.update_cluster(
            project_id=self.project_id,
            location=self.location,
            cluster_name=self.cluster_name,
            cluster=self.cluster,
            update_mask=self.update_mask,
            graceful_decommission_timeout=self.graceful_decommission_timeout,
            request_id=self.request_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        operation.result()
        self.log.info("Updated %s cluster.", self.cluster_name)
