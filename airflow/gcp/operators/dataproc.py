# -*- coding: utf-8 -*-
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

import ntpath
import os
import re
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

from airflow.exceptions import AirflowException
from airflow.gcp.hooks.dataproc import DataProcHook
from airflow.gcp.hooks.gcs import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from airflow.version import version


class DataprocOperationBaseOperator(BaseOperator):
    """
    The base class for operators that poll on a Dataproc Operation.
    """
    @apply_defaults
    def __init__(self,
                 project_id: str,
                 region: str = 'global',
                 gcp_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.project_id = project_id
        self.region = region
        self.hook = DataProcHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version='v1beta2'
        )

    def execute(self, context):
        # pylint: disable=no-value-for-parameter
        self.hook.wait(self.start())

    def start(self, context):
        """
        You are expected to override the method.
        """
        raise AirflowException('Please submit an operation')


# pylint: disable=too-many-instance-attributes
class DataprocClusterCreateOperator(DataprocOperationBaseOperator):
    """
    Create a new cluster on Google Cloud Dataproc. The operator will wait until the
    creation is successful or an error occurs in the creation process.

    The parameters allow to configure the cluster. Please refer to

    https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters

    for a detailed explanation on the different parameters. Most of the configuration
    parameters detailed in the link are available as a parameter to this operator.

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

    template_fields = ['cluster_name', 'project_id', 'zone', 'region']

    # pylint: disable=too-many-arguments,too-many-locals
    @apply_defaults
    def __init__(self,
                 project_id: str,
                 cluster_name: str,
                 num_workers: int,
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
                 region: str = 'global',
                 service_account: Optional[str] = None,
                 service_account_scopes: Optional[List[str]] = None,
                 idle_delete_ttl: Optional[int] = None,
                 auto_delete_time: Optional[datetime] = None,
                 auto_delete_ttl: Optional[int] = None,
                 customer_managed_key: Optional[str] = None,
                 *args,
                 **kwargs) -> None:

        super().__init__(project_id=project_id, region=region, *args, **kwargs)
        self.cluster_name = cluster_name
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

        assert not (self.custom_image and self.image_version), \
            "custom_image and image_version can't be both set"

        assert (
            not self.single_node or (
                self.single_node and self.num_preemptible_workers == 0
            )
        ), "num_workers == 0 means single node mode - no preemptibles allowed"

    def _cluster_ready(self, state, service):
        if state == 'RUNNING':
            return True
        if state == 'DELETING':
            raise Exception('Tried to create a cluster but it\'s in DELETING, something went wrong.')
        if state == 'ERROR':
            cluster = DataProcHook.find_cluster(service, self.project_id, self.region, self.cluster_name)
            try:
                error_details = cluster['status']['details']
            except KeyError:
                error_details = 'Unknown error in cluster creation, ' \
                                'check Google Cloud console for details.'

            self.log.info('Dataproc cluster creation resulted in an ERROR state running diagnostics')
            self.log.info(error_details)
            diagnose_operation_name = \
                DataProcHook.execute_dataproc_diagnose(service, self.project_id,
                                                       self.region, self.cluster_name)
            diagnose_result = DataProcHook.wait_for_operation_done(service, diagnose_operation_name)
            if diagnose_result.get('response') and diagnose_result.get('response').get('outputUri'):
                output_uri = diagnose_result.get('response').get('outputUri')
                self.log.info('Diagnostic information for ERROR cluster available at [%s]', output_uri)
            else:
                self.log.info('Diagnostic information could not be retrieved!')

            raise Exception(error_details)
        return False

    def _get_init_action_timeout(self):
        match = re.match(r"^(\d+)([sm])$", self.init_action_timeout)
        if match:
            if match.group(2) == "s":
                return self.init_action_timeout
            elif match.group(2) == "m":
                val = float(match.group(1))
                return "{}s".format(timedelta(minutes=val).seconds)

        raise AirflowException(
            "DataprocClusterCreateOperator init_action_timeout"
            " should be expressed in minutes or seconds. i.e. 10m, 30s")

    def _build_gce_cluster_config(self, cluster_data):
        if self.zone:
            zone_uri = \
                'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(
                    self.project_id, self.zone
                )
            cluster_data['config']['gceClusterConfig']['zoneUri'] = zone_uri

        if self.metadata:
            cluster_data['config']['gceClusterConfig']['metadata'] = self.metadata

        if self.network_uri:
            cluster_data['config']['gceClusterConfig']['networkUri'] = self.network_uri

        if self.subnetwork_uri:
            cluster_data['config']['gceClusterConfig']['subnetworkUri'] = \
                self.subnetwork_uri

        if self.internal_ip_only:
            if not self.subnetwork_uri:
                raise AirflowException("Set internal_ip_only to true only when"
                                       " you pass a subnetwork_uri.")
            cluster_data['config']['gceClusterConfig']['internalIpOnly'] = True

        if self.tags:
            cluster_data['config']['gceClusterConfig']['tags'] = self.tags

        if self.service_account:
            cluster_data['config']['gceClusterConfig']['serviceAccount'] = \
                self.service_account

        if self.service_account_scopes:
            cluster_data['config']['gceClusterConfig']['serviceAccountScopes'] = \
                self.service_account_scopes

        return cluster_data

    def _build_lifecycle_config(self, cluster_data):
        if self.idle_delete_ttl:
            cluster_data['config']['lifecycleConfig']['idleDeleteTtl'] = \
                "{}s".format(self.idle_delete_ttl)

        if self.auto_delete_time:
            utc_auto_delete_time = timezone.convert_to_utc(self.auto_delete_time)
            cluster_data['config']['lifecycleConfig']['autoDeleteTime'] = \
                utc_auto_delete_time.format('%Y-%m-%dT%H:%M:%S.%fZ', formatter='classic')
        elif self.auto_delete_ttl:
            cluster_data['config']['lifecycleConfig']['autoDeleteTtl'] = \
                "{}s".format(self.auto_delete_ttl)

        return cluster_data

    def _build_cluster_data(self):
        if self.zone:
            master_type_uri = \
                "https://www.googleapis.com/compute/v1/projects/{}/zones/{}/machineTypes/{}"\
                .format(self.project_id, self.zone, self.master_machine_type)
            worker_type_uri = \
                "https://www.googleapis.com/compute/v1/projects/{}/zones/{}/machineTypes/{}"\
                .format(self.project_id, self.zone, self.worker_machine_type)
        else:
            master_type_uri = self.master_machine_type
            worker_type_uri = self.worker_machine_type

        cluster_data = {
            'projectId': self.project_id,
            'clusterName': self.cluster_name,
            'config': {
                'gceClusterConfig': {
                },
                'masterConfig': {
                    'numInstances': self.num_masters,
                    'machineTypeUri': master_type_uri,
                    'diskConfig': {
                        'bootDiskType': self.master_disk_type,
                        'bootDiskSizeGb': self.master_disk_size
                    }
                },
                'workerConfig': {
                    'numInstances': self.num_workers,
                    'machineTypeUri': worker_type_uri,
                    'diskConfig': {
                        'bootDiskType': self.worker_disk_type,
                        'bootDiskSizeGb': self.worker_disk_size
                    }
                },
                'secondaryWorkerConfig': {},
                'softwareConfig': {},
                'lifecycleConfig': {},
                'encryptionConfig': {},
                'autoscalingConfig': {},
            }
        }
        if self.num_preemptible_workers > 0:
            cluster_data['config']['secondaryWorkerConfig'] = {
                'numInstances': self.num_preemptible_workers,
                'machineTypeUri': worker_type_uri,
                'diskConfig': {
                    'bootDiskType': self.worker_disk_type,
                    'bootDiskSizeGb': self.worker_disk_size
                },
                'isPreemptible': True
            }

        cluster_data['labels'] = self.labels or {}

        # Dataproc labels must conform to the following regex:
        # [a-z]([-a-z0-9]*[a-z0-9])? (current airflow version string follows
        # semantic versioning spec: x.y.z).
        cluster_data['labels'].update({'airflow-version':
                                       'v' + version.replace('.', '-').replace('+', '-')})
        if self.storage_bucket:
            cluster_data['config']['configBucket'] = self.storage_bucket

        if self.image_version:
            cluster_data['config']['softwareConfig']['imageVersion'] = self.image_version

        elif self.custom_image:
            project_id = self.custom_image_project_id or self.project_id
            custom_image_url = 'https://www.googleapis.com/compute/beta/projects/' \
                               '{}/global/images/{}'.format(project_id,
                                                            self.custom_image)
            cluster_data['config']['masterConfig']['imageUri'] = custom_image_url
            if not self.single_node:
                cluster_data['config']['workerConfig']['imageUri'] = custom_image_url

        cluster_data = self._build_gce_cluster_config(cluster_data)

        if self.single_node:
            self.properties["dataproc:dataproc.allow.zero.workers"] = "true"

        if self.properties:
            cluster_data['config']['softwareConfig']['properties'] = self.properties

        if self.optional_components:
            cluster_data['config']['softwareConfig']['optionalComponents'] = self.optional_components

        cluster_data = self._build_lifecycle_config(cluster_data)

        if self.init_actions_uris:
            init_actions_dict = [
                {
                    'executableFile': uri,
                    'executionTimeout': self._get_init_action_timeout()
                } for uri in self.init_actions_uris
            ]
            cluster_data['config']['initializationActions'] = init_actions_dict

        if self.customer_managed_key:
            cluster_data['config']['encryptionConfig'] =\
                {'gcePdKmsKeyName': self.customer_managed_key}
        if self.autoscaling_policy:
            cluster_data['config']['autoscalingConfig'] = {'policyUri': self.autoscaling_policy}

        return cluster_data

    def _usable_existing_cluster_present(self, service):
        existing_cluster = DataProcHook.find_cluster(service, self.project_id, self.region, self.cluster_name)
        if existing_cluster:
            self.log.info(
                'Cluster %s already exists... Checking status...',
                self.cluster_name
            )
            existing_status = self.hook.get_final_cluster_state(self.project_id,
                                                                self.region, self.cluster_name, self.log)

            if existing_status == 'RUNNING':
                self.log.info('Cluster exists and is already running. Using it.')
                return True

            elif existing_status == 'DELETING':
                while DataProcHook.find_cluster(service, self.project_id, self.region, self.cluster_name) \
                    and DataProcHook.get_cluster_state(service, self.project_id,
                                                       self.region, self.cluster_name) == 'DELETING':
                    self.log.info('Existing cluster is deleting, waiting for it to finish')
                    time.sleep(15)

            elif existing_status == 'ERROR':
                self.log.info('Existing cluster in ERROR state, deleting it first')

                operation_name = DataProcHook.execute_delete(service, self.project_id,
                                                             self.region, self.cluster_name)
                self.log.info("Cluster delete operation name: %s", operation_name)
                DataProcHook.wait_for_operation_done_or_error(service, operation_name)

        return False

    def start(self):
        """
        Create a new cluster on Google Cloud Dataproc.
        """
        self.log.info('Creating cluster: %s', self.cluster_name)
        hook = DataProcHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )
        service = hook.get_conn()

        if self._usable_existing_cluster_present(service):
            return True

        cluster_data = self._build_cluster_data()

        return (
            self.hook.get_conn().projects().regions().clusters().create(  # pylint: disable=no-member
                projectId=self.project_id,
                region=self.region,
                body=cluster_data,
                requestId=str(uuid.uuid4()),
            ).execute())


class DataprocClusterScaleOperator(DataprocOperationBaseOperator):
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
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param num_workers: The new number of workers
    :type num_workers: int
    :param num_preemptible_workers: The new number of preemptible workers
    :type num_preemptible_workers: int
    :param graceful_decommission_timeout: Timeout for graceful YARN decomissioning.
        Maximum value is 1d
    :type graceful_decommission_timeout: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    """

    template_fields = ['cluster_name', 'project_id', 'region']

    @apply_defaults
    def __init__(self,
                 cluster_name: str,
                 project_id: str,
                 region: str = 'global',
                 num_workers: int = 2,
                 num_preemptible_workers: int = 0,
                 graceful_decommission_timeout: Optional[str] = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(project_id=project_id, region=region, *args, **kwargs)
        self.cluster_name = cluster_name
        self.num_workers = num_workers
        self.num_preemptible_workers = num_preemptible_workers

        # Optional
        self.optional_arguments = {}  # type: Dict
        if graceful_decommission_timeout:
            self.optional_arguments['gracefulDecommissionTimeout'] = \
                self._get_graceful_decommission_timeout(
                    graceful_decommission_timeout)

    def _build_scale_cluster_data(self):
        scale_data = {
            'config': {
                'workerConfig': {
                    'numInstances': self.num_workers
                },
                'secondaryWorkerConfig': {
                    'numInstances': self.num_preemptible_workers
                }
            }
        }
        return scale_data

    @staticmethod
    def _get_graceful_decommission_timeout(timeout):
        match = re.match(r"^(\d+)([smdh])$", timeout)
        if match:
            if match.group(2) == "s":
                return timeout
            elif match.group(2) == "m":
                val = float(match.group(1))
                return "{}s".format(timedelta(minutes=val).seconds)
            elif match.group(2) == "h":
                val = float(match.group(1))
                return "{}s".format(timedelta(hours=val).seconds)
            elif match.group(2) == "d":
                val = float(match.group(1))
                return "{}s".format(timedelta(days=val).seconds)

        raise AirflowException(
            "DataprocClusterScaleOperator "
            " should be expressed in day, hours, minutes or seconds. "
            " i.e. 1d, 4h, 10m, 30s")

    def start(self):
        """
        Scale, up or down, a cluster on Google Cloud Dataproc.
        """
        self.log.info("Scaling cluster: %s", self.cluster_name)

        update_mask = "config.worker_config.num_instances," \
                      + "config.secondary_worker_config.num_instances"
        scaling_cluster_data = self._build_scale_cluster_data()

        return (
            self.hook.get_conn().projects().regions().clusters().patch(  # pylint: disable=no-member
                projectId=self.project_id,
                region=self.region,
                clusterName=self.cluster_name,
                updateMask=update_mask,
                body=scaling_cluster_data,
                requestId=str(uuid.uuid4()),
                **self.optional_arguments
            ).execute())


class DataprocClusterDeleteOperator(DataprocOperationBaseOperator):
    """
    Delete a cluster on Google Cloud Dataproc. The operator will wait until the
    cluster is destroyed.

    :param cluster_name: The name of the cluster to delete. (templated)
    :type cluster_name: str
    :param project_id: The ID of the google cloud project in which
        the cluster runs. (templated)
    :type project_id: str
    :param region: leave as 'global', might become relevant in the future. (templated)
    :type region: str
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    """

    template_fields = ['cluster_name', 'project_id', 'region']

    @apply_defaults
    def __init__(self,
                 cluster_name: str,
                 project_id: str,
                 region: str = 'global',
                 *args,
                 **kwargs) -> None:

        super().__init__(project_id=project_id, region=region, *args, **kwargs)
        self.cluster_name = cluster_name

    def start(self):
        """
        Delete a cluster on Google Cloud Dataproc.
        """
        self.log.info('Deleting cluster: %s in %s', self.cluster_name, self.region)
        return (
            self.hook.get_conn().projects().regions().clusters().delete(  # pylint: disable=no-member
                projectId=self.project_id,
                region=self.region,
                clusterName=self.cluster_name,
                requestId=str(uuid.uuid4()),
            ).execute())


class DataProcJobBaseOperator(BaseOperator):
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

        self.hook = DataProcHook(gcp_conn_id=gcp_conn_id,
                                 delegate_to=delegate_to)
        self.job_template = None
        self.job = None
        self.dataproc_job_id = None

    def create_job_template(self):
        """
        Initialize `self.job_template` with default values
        """
        self.job_template = self.hook.create_job_template(self.task_id, self.cluster_name, self.job_type,
                                                          self.dataproc_properties)
        self.job_template.set_job_name(self.job_name)
        self.job_template.add_jar_file_uris(self.dataproc_jars)
        self.job_template.add_labels(self.labels)

    def execute(self, context):
        if self.job_template:
            self.job = self.job_template.build()
            self.dataproc_job_id = self.job["job"]["reference"]["jobId"]
            self.hook.submit(self.hook.project_id, self.job, self.region, self.job_error_states)
        else:
            raise AirflowException("Create a job template before")

    def on_kill(self):
        """
        Callback called when the operator is killed.
        Cancel any running job.
        """
        if self.dataproc_job_id:
            self.hook.cancel(self.hook.project_id, self.dataproc_job_id, self.region)


class DataProcPigOperator(DataProcJobBaseOperator):
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
    job_type = 'pigJob'

    @apply_defaults
    def __init__(
            self,
            query: Optional[str] = None,
            query_uri: Optional[str] = None,
            variables: Optional[Dict] = None,
            *args,
            **kwargs) -> None:

        super().__init__(*args, **kwargs)
        self.query = query
        self.query_uri = query_uri
        self.variables = variables

    def execute(self, context):
        self.create_job_template()

        if self.query is None:
            self.job_template.add_query_uri(self.query_uri)
        else:
            self.job_template.add_query(self.query)
        self.job_template.add_variables(self.variables)

        super().execute(context)


class DataProcHiveOperator(DataProcJobBaseOperator):
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
    job_type = 'hiveJob'

    @apply_defaults
    def __init__(
            self,
            query: Optional[str] = None,
            query_uri: Optional[str] = None,
            variables: Optional[Dict] = None,
            *args,
            **kwargs) -> None:

        super().__init__(*args, **kwargs)
        self.query = query
        self.query_uri = query_uri
        self.variables = variables
        if self.query is not None and self.query_uri is not None:
            raise AirflowException('Only one of `query` and `query_uri` can be passed.')

    def execute(self, context):
        self.create_job_template()
        if self.query is None:
            self.job_template.add_query_uri(self.query_uri)
        else:
            self.job_template.add_query(self.query)
        self.job_template.add_variables(self.variables)

        super().execute(context)


class DataProcSparkSqlOperator(DataProcJobBaseOperator):
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
    job_type = 'sparkSqlJob'

    @apply_defaults
    def __init__(
            self,
            query: Optional[str] = None,
            query_uri: Optional[str] = None,
            variables: Optional[Dict] = None,
            *args,
            **kwargs) -> None:

        super().__init__(*args, **kwargs)
        self.query = query
        self.query_uri = query_uri
        self.variables = variables
        if self.query is not None and self.query_uri is not None:
            raise AirflowException('Only one of `query` and `query_uri` can be passed.')

    def execute(self, context):
        self.create_job_template()
        if self.query is None:
            self.job_template.add_query_uri(self.query_uri)
        else:
            self.job_template.add_query(self.query)
        self.job_template.add_variables(self.variables)

        super().execute(context)


class DataProcSparkOperator(DataProcJobBaseOperator):
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
    job_type = 'sparkJob'

    @apply_defaults
    def __init__(
            self,
            main_jar: Optional[str] = None,
            main_class: Optional[str] = None,
            arguments: Optional[List] = None,
            archives: Optional[List] = None,
            files: Optional[List] = None,
            *args,
            **kwargs) -> None:

        super().__init__(*args, **kwargs)
        self.main_jar = main_jar
        self.main_class = main_class
        self.arguments = arguments
        self.archives = archives
        self.files = files

    def execute(self, context):
        self.create_job_template()
        self.job_template.set_main(self.main_jar, self.main_class)
        self.job_template.add_args(self.arguments)
        self.job_template.add_archive_uris(self.archives)
        self.job_template.add_file_uris(self.files)

        super().execute(context)


class DataProcHadoopOperator(DataProcJobBaseOperator):
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
    job_type = 'hadoopJob'

    @apply_defaults
    def __init__(
            self,
            main_jar: Optional[str] = None,
            main_class: Optional[str] = None,
            arguments: Optional[List] = None,
            archives: Optional[List] = None,
            files: Optional[List] = None,
            *args,
            **kwargs) -> None:

        super().__init__(*args, **kwargs)
        self.main_jar = main_jar
        self.main_class = main_class
        self.arguments = arguments
        self.archives = archives
        self.files = files

    def execute(self, context):
        self.create_job_template()
        self.job_template.set_main(self.main_jar, self.main_class)
        self.job_template.add_args(self.arguments)
        self.job_template.add_archive_uris(self.archives)
        self.job_template.add_file_uris(self.files)

        super().execute(context)


class DataProcPySparkOperator(DataProcJobBaseOperator):
    """
    Start a PySpark Job on a Cloud DataProc cluster.

    :param main: [Required] The Hadoop Compatible Filesystem (HCFS) URI of the main
            Python file to use as the driver. Must be a .py file.
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

    template_fields = ['arguments', 'job_name', 'cluster_name',
                       'region', 'dataproc_jars', 'dataproc_properties']
    ui_color = '#0273d4'
    job_type = 'pysparkJob'

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

        GoogleCloudStorageHook(
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
            pyfiles: Optional[Optional[Optional[List]]] = None,
            files: Optional[List] = None,
            *args,
            **kwargs) -> None:

        super().__init__(*args, **kwargs)
        self.main = main
        self.arguments = arguments
        self.archives = archives
        self.files = files
        self.pyfiles = pyfiles

    def execute(self, context):
        self.create_job_template()
        #  Check if the file is local, if that is the case, upload it to a bucket
        if os.path.isfile(self.main):
            cluster_info = self.hook.get_cluster(
                project_id=self.hook.project_id,
                region=self.region,
                cluster_name=self.cluster_name
            )
            bucket = cluster_info['config']['configBucket']
            self.main = self._upload_file_temp(bucket, self.main)

        self.job_template.set_python_main(self.main)
        self.job_template.add_args(self.arguments)
        self.job_template.add_archive_uris(self.archives)
        self.job_template.add_file_uris(self.files)
        self.job_template.add_python_file_uris(self.pyfiles)

        super().execute(context)


class DataprocWorkflowTemplateInstantiateOperator(DataprocOperationBaseOperator):
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
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
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
    """

    template_fields = ['template_id']

    @apply_defaults
    def __init__(self, template_id: str, parameters: Dict[str, str], *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.template_id = template_id
        self.parameters = parameters

    def start(self):
        """
        Instantiate a WorkflowTemplate on Google Cloud Dataproc with given parameters.
        """
        self.log.info('Instantiating Template: %s', self.template_id)
        return (
            self.hook.get_conn().projects().regions().workflowTemplates()  # pylint: disable=no-member
            .instantiate(
                name=('projects/%s/regions/%s/workflowTemplates/%s' %
                      (self.project_id, self.region, self.template_id)),
                body={'requestId': str(uuid.uuid4()), 'parameters': self.parameters})
            .execute())


class DataprocWorkflowTemplateInstantiateInlineOperator(
        DataprocOperationBaseOperator):
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
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    """

    template_fields = ['template']

    @apply_defaults
    def __init__(self, template: Dict, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.template = template

    def start(self):
        """
        Instantiate a WorkflowTemplate Inline on Google Cloud Dataproc.
        """
        self.log.info('Instantiating Inline Template')
        return (
            self.hook.get_conn().projects().regions().workflowTemplates()  # pylint: disable=no-member
            .instantiateInline(
                parent='projects/%s/regions/%s' % (self.project_id, self.region),
                requestId=str(uuid.uuid4()),
                body=self.template)
            .execute())
