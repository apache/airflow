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

import ntpath
import os
import re
import time
import uuid
from datetime import timedelta

from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.version import version
from googleapiclient.errors import HttpError
from airflow.utils import timezone


class DataprocClusterCreateOperator(BaseOperator):
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
    :param properties: dict of properties to set on
        config files (e.g. spark-defaults.conf), see
        https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters#SoftwareConfig
    :type properties: dict
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
        (projects/[PROJECT_STORING_KEYS]/locations/[LOCATION]/keyRings/[KEY_RING_NAME]/cryptoKeys/[KEY_NAME])
    :type customer_managed_key: str
    """

    template_fields = ['cluster_name', 'project_id', 'zone', 'region']

    @apply_defaults
    def __init__(self,
                 cluster_name,
                 project_id,
                 num_workers,
                 zone=None,
                 network_uri=None,
                 subnetwork_uri=None,
                 internal_ip_only=None,
                 tags=None,
                 storage_bucket=None,
                 init_actions_uris=None,
                 init_action_timeout="10m",
                 metadata=None,
                 custom_image=None,
                 image_version=None,
                 properties=None,
                 master_machine_type='n1-standard-4',
                 master_disk_type='pd-standard',
                 master_disk_size=1024,
                 worker_machine_type='n1-standard-4',
                 worker_disk_type='pd-standard',
                 worker_disk_size=1024,
                 num_preemptible_workers=0,
                 labels=None,
                 region='global',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 service_account=None,
                 service_account_scopes=None,
                 idle_delete_ttl=None,
                 auto_delete_time=None,
                 auto_delete_ttl=None,
                 customer_managed_key=None,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.cluster_name = cluster_name
        self.project_id = project_id
        self.num_workers = num_workers
        self.num_preemptible_workers = num_preemptible_workers
        self.storage_bucket = storage_bucket
        self.init_actions_uris = init_actions_uris
        self.init_action_timeout = init_action_timeout
        self.metadata = metadata
        self.custom_image = custom_image
        self.image_version = image_version
        self.properties = properties or dict()
        self.master_machine_type = master_machine_type
        self.master_disk_type = master_disk_type
        self.master_disk_size = master_disk_size
        self.worker_machine_type = worker_machine_type
        self.worker_disk_type = worker_disk_type
        self.worker_disk_size = worker_disk_size
        self.labels = labels
        self.zone = zone
        self.network_uri = network_uri
        self.subnetwork_uri = subnetwork_uri
        self.internal_ip_only = internal_ip_only
        self.tags = tags
        self.region = region
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

    def _get_cluster_list_for_project(self, service):
        result = service.projects().regions().clusters().list(
            projectId=self.project_id,
            region=self.region
        ).execute()
        return result.get('clusters', [])

    def _get_cluster(self, service):
        cluster_list = self._get_cluster_list_for_project(service)
        cluster = [c for c in cluster_list if c['clusterName'] == self.cluster_name]
        if cluster:
            return cluster[0]
        return None

    def _get_cluster_state(self, service):
        cluster = self._get_cluster(service)
        if 'status' in cluster:
            return cluster['status']['state']
        else:
            return None

    def _cluster_ready(self, state, service):
        if state == 'RUNNING':
            return True
        if state == 'ERROR':
            cluster = self._get_cluster(service)
            try:
                error_details = cluster['status']['details']
            except KeyError:
                error_details = 'Unknown error in cluster creation, ' \
                                'check Google Cloud console for details.'
            raise Exception(error_details)
        return False

    def _wait_for_done(self, service):
        while True:
            state = self._get_cluster_state(service)
            if state is None:
                self.log.info("No state for cluster '%s'", self.cluster_name)
                time.sleep(15)
            else:
                self.log.info("State for cluster '%s' is %s", self.cluster_name, state)
                if self._cluster_ready(state, service):
                    self.log.info(
                        "Cluster '%s' successfully created", self.cluster_name
                    )
                    return
                time.sleep(15)

    def _get_init_action_timeout(self):
        match = re.match(r"^(\d+)(s|m)$", self.init_action_timeout)
        if match:
            if match.group(2) == "s":
                return self.init_action_timeout
            elif match.group(2) == "m":
                val = float(match.group(1))
                return "{}s".format(timedelta(minutes=val).seconds)

        raise AirflowException(
            "DataprocClusterCreateOperator init_action_timeout"
            " should be expressed in minutes or seconds. i.e. 10m, 30s")

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
                    'numInstances': 1,
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
                'encryptionConfig': {}
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

        cluster_data['labels'] = self.labels if self.labels else {}
        # Dataproc labels must conform to the following regex:
        # [a-z]([-a-z0-9]*[a-z0-9])? (current airflow version string follows
        # semantic versioning spec: x.y.z).
        cluster_data['labels'].update({'airflow-version':
                                       'v' + version.replace('.', '-').replace('+', '-')})
        if self.storage_bucket:
            cluster_data['config']['configBucket'] = self.storage_bucket
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
        if self.image_version:
            cluster_data['config']['softwareConfig']['imageVersion'] = self.image_version
        elif self.custom_image:
            custom_image_url = 'https://www.googleapis.com/compute/beta/projects/' \
                               '{}/global/images/{}'.format(self.project_id,
                                                            self.custom_image)
            cluster_data['config']['masterConfig']['imageUri'] = custom_image_url
            if not self.single_node:
                cluster_data['config']['workerConfig']['imageUri'] = custom_image_url

        if self.single_node:
            self.properties["dataproc:dataproc.allow.zero.workers"] = "true"

        if self.properties:
            cluster_data['config']['softwareConfig']['properties'] = self.properties
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
        if self.init_actions_uris:
            init_actions_dict = [
                {
                    'executableFile': uri,
                    'executionTimeout': self._get_init_action_timeout()
                } for uri in self.init_actions_uris
            ]
            cluster_data['config']['initializationActions'] = init_actions_dict
        if self.service_account:
            cluster_data['config']['gceClusterConfig']['serviceAccount'] =\
                self.service_account
        if self.service_account_scopes:
            cluster_data['config']['gceClusterConfig']['serviceAccountScopes'] =\
                self.service_account_scopes
        if self.customer_managed_key:
            cluster_data['config']['encryptionConfig'] =\
                {'gcePdKmsKeyName': self.customer_managed_key}
        return cluster_data

    def execute(self, context):
        self.log.info('Creating cluster: %s', self.cluster_name)
        hook = DataProcHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )
        service = hook.get_conn()

        if self._get_cluster(service):
            self.log.info(
                'Cluster %s already exists... Checking status...',
                self.cluster_name
            )
            self._wait_for_done(service)
            return True

        cluster_data = self._build_cluster_data()
        try:
            service.projects().regions().clusters().create(
                projectId=self.project_id,
                region=self.region,
                body=cluster_data
            ).execute()
        except HttpError as e:
            # probably two cluster start commands at the same time
            time.sleep(10)
            if self._get_cluster(service):
                self.log.info(
                    'Cluster {} already exists... Checking status...',
                    self.cluster_name
                )
                self._wait_for_done(service)
                return True
            else:
                raise e

        self._wait_for_done(service)


class DataprocClusterScaleOperator(BaseOperator):
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
                 cluster_name,
                 project_id,
                 region='global',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 num_workers=2,
                 num_preemptible_workers=0,
                 graceful_decommission_timeout=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.cluster_name = cluster_name
        self.project_id = project_id
        self.region = region
        self.num_workers = num_workers
        self.num_preemptible_workers = num_preemptible_workers

        # Optional
        self.optional_arguments = {}
        if graceful_decommission_timeout:
            self.optional_arguments['gracefulDecommissionTimeout'] = \
                self._get_graceful_decommission_timeout(
                    graceful_decommission_timeout)

    def _wait_for_done(self, service, operation_name):
        time.sleep(15)
        while True:
            try:
                response = service.projects().regions().operations().get(
                    name=operation_name
                ).execute()

                if 'done' in response and response['done']:
                    if 'error' in response:
                        raise Exception(str(response['error']))
                    else:
                        return
                time.sleep(15)
            except HttpError as e:
                self.log.error("Operation not found.")
                raise e

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
        match = re.match(r"^(\d+)(s|m|h|d)$", timeout)
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

    def execute(self, context):
        self.log.info("Scaling cluster: %s", self.cluster_name)
        hook = DataProcHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )
        service = hook.get_conn()

        update_mask = "config.worker_config.num_instances," \
                      + "config.secondary_worker_config.num_instances"
        scaling_cluster_data = self._build_scale_cluster_data()

        response = service.projects().regions().clusters().patch(
            projectId=self.project_id,
            region=self.region,
            clusterName=self.cluster_name,
            updateMask=update_mask,
            body=scaling_cluster_data,
            **self.optional_arguments
        ).execute()
        operation_name = response['name']
        self.log.info("Cluster scale operation name: %s", operation_name)
        self._wait_for_done(service, operation_name)


class DataprocClusterDeleteOperator(BaseOperator):
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
                 cluster_name,
                 project_id,
                 region='global',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.cluster_name = cluster_name
        self.project_id = project_id
        self.region = region

    @staticmethod
    def _wait_for_done(service, operation_name):
        time.sleep(15)
        while True:
            response = service.projects().regions().operations().get(
                name=operation_name
            ).execute()

            if 'done' in response and response['done']:
                if 'error' in response:
                    raise Exception(str(response['error']))
                else:
                    return
            time.sleep(15)

    def execute(self, context):
        self.log.info('Deleting cluster: %s', self.cluster_name)
        hook = DataProcHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )
        service = hook.get_conn()

        response = service.projects().regions().clusters().delete(
            projectId=self.project_id,
            region=self.region,
            clusterName=self.cluster_name
        ).execute()
        operation_name = response['name']
        self.log.info("Cluster delete operation name: %s", operation_name)
        self._wait_for_done(service, operation_name)


class DataProcPigOperator(BaseOperator):
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
    :param query_uri: The uri of a pig script on Cloud Storage.
    :type query_uri: str
    :param variables: Map of named parameters for the query. (templated)
    :type variables: dict
    :param job_name: The job name used in the DataProc cluster. This
        name by default is the task_id appended with the execution data, but can
        be templated. The name will always be appended with a random number to
        avoid name clashes. (templated)
    :type job_name: str
    :param cluster_name: The name of the DataProc cluster. (templated)
    :type cluster_name: str
    :param dataproc_pig_properties: Map for the Pig properties. Ideal to put in
        default arguments
    :type dataproc_pig_properties: dict
    :param dataproc_pig_jars: HCFS URIs of jar files to add to the CLASSPATH of the Pig Client and Hadoop
        MapReduce (MR) tasks. Can contain Pig UDFs. (templated)
    :type dataproc_pig_jars: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
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
    template_fields = ['query', 'variables', 'job_name', 'cluster_name', 'region', 'dataproc_jars']
    template_ext = ('.pg', '.pig',)
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            query=None,
            query_uri=None,
            variables=None,
            job_name='{{task.task_id}}_{{ds_nodash}}',
            cluster_name='cluster-1',
            dataproc_pig_properties=None,
            dataproc_pig_jars=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            region='global',
            job_error_states=None,
            *args,
            **kwargs):

        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.query = query
        self.query_uri = query_uri
        self.variables = variables
        self.job_name = job_name
        self.cluster_name = cluster_name
        self.dataproc_properties = dataproc_pig_properties
        self.dataproc_jars = dataproc_pig_jars
        self.region = region
        self.job_error_states = job_error_states if job_error_states is not None else {'ERROR'}

    def execute(self, context):
        hook = DataProcHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)
        job = hook.create_job_template(self.task_id, self.cluster_name, "pigJob",
                                       self.dataproc_properties)

        if self.query is None:
            job.add_query_uri(self.query_uri)
        else:
            job.add_query(self.query)
        job.add_variables(self.variables)
        job.add_jar_file_uris(self.dataproc_jars)
        job.set_job_name(self.job_name)

        job_to_submit = job.build()
        self.dataproc_job_id = job_to_submit["job"]["reference"]["jobId"]

        hook.submit(hook.project_id, job_to_submit, self.region, self.job_error_states)


class DataProcHiveOperator(BaseOperator):
    """
    Start a Hive query Job on a Cloud DataProc cluster.

    :param query: The query or reference to the query file (q extension).
    :type query: str
    :param query_uri: The uri of a hive script on Cloud Storage.
    :type query_uri: str
    :param variables: Map of named parameters for the query.
    :type variables: dict
    :param job_name: The job name used in the DataProc cluster. This name by default
        is the task_id appended with the execution data, but can be templated. The
        name will always be appended with a random number to avoid name clashes.
    :type job_name: str
    :param cluster_name: The name of the DataProc cluster.
    :type cluster_name: str
    :param dataproc_hive_properties: Map for the Pig properties. Ideal to put in
        default arguments
    :type dataproc_hive_properties: dict
    :param dataproc_hive_jars: HCFS URIs of jar files to add to the CLASSPATH of the Hive server and Hadoop
        MapReduce (MR) tasks. Can contain Hive SerDes and UDFs. (templated)
    :type dataproc_hive_jars: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
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
    template_fields = ['query', 'variables', 'job_name', 'cluster_name', 'region', 'dataproc_jars']
    template_ext = ('.q',)
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            query=None,
            query_uri=None,
            variables=None,
            job_name='{{task.task_id}}_{{ds_nodash}}',
            cluster_name='cluster-1',
            dataproc_hive_properties=None,
            dataproc_hive_jars=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            region='global',
            job_error_states=None,
            *args,
            **kwargs):

        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.query = query
        self.query_uri = query_uri
        self.variables = variables
        self.job_name = job_name
        self.cluster_name = cluster_name
        self.dataproc_properties = dataproc_hive_properties
        self.dataproc_jars = dataproc_hive_jars
        self.region = region
        self.job_error_states = job_error_states if job_error_states is not None else {'ERROR'}

    def execute(self, context):
        hook = DataProcHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)

        job = hook.create_job_template(self.task_id, self.cluster_name, "hiveJob",
                                       self.dataproc_properties)

        if self.query is None:
            job.add_query_uri(self.query_uri)
        else:
            job.add_query(self.query)
        job.add_variables(self.variables)
        job.add_jar_file_uris(self.dataproc_jars)
        job.set_job_name(self.job_name)

        job_to_submit = job.build()
        self.dataproc_job_id = job_to_submit["job"]["reference"]["jobId"]

        hook.submit(hook.project_id, job_to_submit, self.region, self.job_error_states)


class DataProcSparkSqlOperator(BaseOperator):
    """
    Start a Spark SQL query Job on a Cloud DataProc cluster.

    :param query: The query or reference to the query file (q extension). (templated)
    :type query: str
    :param query_uri: The uri of a spark sql script on Cloud Storage.
    :type query_uri: str
    :param variables: Map of named parameters for the query. (templated)
    :type variables: dict
    :param job_name: The job name used in the DataProc cluster. This
        name by default is the task_id appended with the execution data, but can
        be templated. The name will always be appended with a random number to
        avoid name clashes. (templated)
    :type job_name: str
    :param cluster_name: The name of the DataProc cluster. (templated)
    :type cluster_name: str
    :param dataproc_spark_properties: Map for the Pig properties. Ideal to put in
        default arguments
    :type dataproc_spark_properties: dict
    :param dataproc_spark_jars: HCFS URIs of jar files to be added to the Spark CLASSPATH. (templated)
    :type dataproc_spark_jars: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
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
    template_fields = ['query', 'variables', 'job_name', 'cluster_name', 'region', 'dataproc_jars']
    template_ext = ('.q',)
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            query=None,
            query_uri=None,
            variables=None,
            job_name='{{task.task_id}}_{{ds_nodash}}',
            cluster_name='cluster-1',
            dataproc_spark_properties=None,
            dataproc_spark_jars=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            region='global',
            job_error_states=None,
            *args,
            **kwargs):

        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.query = query
        self.query_uri = query_uri
        self.variables = variables
        self.job_name = job_name
        self.cluster_name = cluster_name
        self.dataproc_properties = dataproc_spark_properties
        self.dataproc_jars = dataproc_spark_jars
        self.region = region
        self.job_error_states = job_error_states if job_error_states is not None else {'ERROR'}

    def execute(self, context):
        hook = DataProcHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)

        job = hook.create_job_template(self.task_id, self.cluster_name, "sparkSqlJob",
                                       self.dataproc_properties)

        if self.query is None:
            job.add_query_uri(self.query_uri)
        else:
            job.add_query(self.query)
        job.add_variables(self.variables)
        job.add_jar_file_uris(self.dataproc_jars)
        job.set_job_name(self.job_name)

        job_to_submit = job.build()
        self.dataproc_job_id = job_to_submit["job"]["reference"]["jobId"]

        hook.submit(hook.project_id, job_to_submit, self.region, self.job_error_states)


class DataProcSparkOperator(BaseOperator):
    """
    Start a Spark Job on a Cloud DataProc cluster.

    :param main_jar: URI of the job jar provisioned on Cloud Storage. (use this or
            the main_class, not both together).
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
    :param job_name: The job name used in the DataProc cluster. This
        name by default is the task_id appended with the execution data, but can
        be templated. The name will always be appended with a random number to
        avoid name clashes. (templated)
    :type job_name: str
    :param cluster_name: The name of the DataProc cluster. (templated)
    :type cluster_name: str
    :param dataproc_spark_properties: Map for the Pig properties. Ideal to put in
        default arguments
    :type dataproc_spark_properties: dict
    :param dataproc_spark_jars: HCFS URIs of files to be copied to the working directory of Spark drivers
        and distributed tasks. Useful for naively parallel tasks. (templated)
    :type dataproc_spark_jars: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
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

    template_fields = ['arguments', 'job_name', 'cluster_name', 'region', 'dataproc_jars']
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            main_jar=None,
            main_class=None,
            arguments=None,
            archives=None,
            files=None,
            job_name='{{task.task_id}}_{{ds_nodash}}',
            cluster_name='cluster-1',
            dataproc_spark_properties=None,
            dataproc_spark_jars=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            region='global',
            job_error_states=None,
            *args,
            **kwargs):

        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.main_jar = main_jar
        self.main_class = main_class
        self.arguments = arguments
        self.archives = archives
        self.files = files
        self.job_name = job_name
        self.cluster_name = cluster_name
        self.dataproc_properties = dataproc_spark_properties
        self.dataproc_jars = dataproc_spark_jars
        self.region = region
        self.job_error_states = job_error_states if job_error_states is not None else {'ERROR'}

    def execute(self, context):
        hook = DataProcHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)
        job = hook.create_job_template(self.task_id, self.cluster_name, "sparkJob",
                                       self.dataproc_properties)

        job.set_main(self.main_jar, self.main_class)
        job.add_args(self.arguments)
        job.add_jar_file_uris(self.dataproc_jars)
        job.add_archive_uris(self.archives)
        job.add_file_uris(self.files)
        job.set_job_name(self.job_name)

        job_to_submit = job.build()
        self.dataproc_job_id = job_to_submit["job"]["reference"]["jobId"]

        hook.submit(hook.project_id, job_to_submit, self.region, self.job_error_states)


class DataProcHadoopOperator(BaseOperator):
    """
    Start a Hadoop Job on a Cloud DataProc cluster.

    :param main_jar: URI of the job jar provisioned on Cloud Storage. (use this or
            the main_class, not both together).
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
    :param job_name: The job name used in the DataProc cluster. This
        name by default is the task_id appended with the execution data, but can
        be templated. The name will always be appended with a random number to
        avoid name clashes. (templated)
    :type job_name: str
    :param cluster_name: The name of the DataProc cluster. (templated)
    :type cluster_name: str
    :param dataproc_hadoop_properties: Map for the Pig properties. Ideal to put in
        default arguments
    :type dataproc_hadoop_properties: dict
    :param dataproc_hadoop_jars: Jar file URIs to add to the CLASSPATHs of the Hadoop driver and
        tasks. (tempplated)
    :type dataproc_hadoop_jars: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
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

    template_fields = ['arguments', 'job_name', 'cluster_name', 'region', 'dataproc_jars']
    ui_color = '#0273d4'

    @apply_defaults
    def __init__(
            self,
            main_jar=None,
            main_class=None,
            arguments=None,
            archives=None,
            files=None,
            job_name='{{task.task_id}}_{{ds_nodash}}',
            cluster_name='cluster-1',
            dataproc_hadoop_properties=None,
            dataproc_hadoop_jars=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            region='global',
            job_error_states=None,
            *args,
            **kwargs):

        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.main_jar = main_jar
        self.main_class = main_class
        self.arguments = arguments
        self.archives = archives
        self.files = files
        self.job_name = job_name
        self.cluster_name = cluster_name
        self.dataproc_properties = dataproc_hadoop_properties
        self.dataproc_jars = dataproc_hadoop_jars
        self.region = region
        self.job_error_states = job_error_states if job_error_states is not None else {'ERROR'}

    def execute(self, context):
        hook = DataProcHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)
        job = hook.create_job_template(self.task_id, self.cluster_name, "hadoopJob",
                                       self.dataproc_properties)

        job.set_main(self.main_jar, self.main_class)
        job.add_args(self.arguments)
        job.add_jar_file_uris(self.dataproc_jars)
        job.add_archive_uris(self.archives)
        job.add_file_uris(self.files)
        job.set_job_name(self.job_name)

        job_to_submit = job.build()
        self.dataproc_job_id = job_to_submit["job"]["reference"]["jobId"]

        hook.submit(hook.project_id, job_to_submit, self.region, self.job_error_states)


class DataProcPySparkOperator(BaseOperator):
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
    :param job_name: The job name used in the DataProc cluster. This
        name by default is the task_id appended with the execution data, but can
        be templated. The name will always be appended with a random number to
        avoid name clashes. (templated)
    :type job_name: str
    :param cluster_name: The name of the DataProc cluster.
    :type cluster_name: str
    :param dataproc_pyspark_properties: Map for the Pig properties. Ideal to put in
        default arguments
    :type dataproc_pyspark_properties: dict
    :param dataproc_pyspark_jars: HCFS URIs of jar files to add to the CLASSPATHs of the Python
        driver and tasks. (templated)
    :type dataproc_pyspark_jars: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
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

    template_fields = ['arguments', 'job_name', 'cluster_name', 'region', 'dataproc_jars']
    ui_color = '#0273d4'

    @staticmethod
    def _generate_temp_filename(filename):
        dt = time.strftime('%Y%m%d%H%M%S')
        return "{}_{}_{}".format(dt, str(uuid.uuid4())[:8], ntpath.basename(filename))

    """
    Upload a local file to a Google Cloud Storage bucket
    """
    def _upload_file_temp(self, bucket, local_file):
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
            main,
            arguments=None,
            archives=None,
            pyfiles=None,
            files=None,
            job_name='{{task.task_id}}_{{ds_nodash}}',
            cluster_name='cluster-1',
            dataproc_pyspark_properties=None,
            dataproc_pyspark_jars=None,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            region='global',
            job_error_states=None,
            *args,
            **kwargs):

        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.main = main
        self.arguments = arguments
        self.archives = archives
        self.files = files
        self.pyfiles = pyfiles
        self.job_name = job_name
        self.cluster_name = cluster_name
        self.dataproc_properties = dataproc_pyspark_properties
        self.dataproc_jars = dataproc_pyspark_jars
        self.region = region
        self.job_error_states = job_error_states if job_error_states is not None else {'ERROR'}

    def execute(self, context):
        hook = DataProcHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to
        )
        job = hook.create_job_template(
            self.task_id, self.cluster_name, "pysparkJob", self.dataproc_properties)

        #  Check if the file is local, if that is the case, upload it to a bucket
        if os.path.isfile(self.main):
            cluster_info = hook.get_cluster(
                project_id=hook.project_id,
                region=self.region,
                cluster_name=self.cluster_name
            )
            bucket = cluster_info['config']['configBucket']
            self.main = self._upload_file_temp(bucket, self.main)
        job.set_python_main(self.main)

        job.add_args(self.arguments)
        job.add_jar_file_uris(self.dataproc_jars)
        job.add_archive_uris(self.archives)
        job.add_file_uris(self.files)
        job.add_python_file_uris(self.pyfiles)
        job.set_job_name(self.job_name)

        job_to_submit = job.build()
        self.dataproc_job_id = job_to_submit["job"]["reference"]["jobId"]

        hook.submit(hook.project_id, job_to_submit, self.region, self.job_error_states)


class DataprocWorkflowTemplateBaseOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 project_id,
                 region='global',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
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
        self.hook.wait(self.start())

    def start(self, context):
        raise AirflowException('Please start a workflow operation')


class DataprocWorkflowTemplateInstantiateOperator(DataprocWorkflowTemplateBaseOperator):
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
    """

    template_fields = ['template_id']

    @apply_defaults
    def __init__(self, template_id, *args, **kwargs):
        (super()
            .__init__(*args, **kwargs))
        self.template_id = template_id

    def start(self):
        self.log.info('Instantiating Template: %s', self.template_id)
        return (
            self.hook.get_conn().projects().regions().workflowTemplates()
            .instantiate(
                name=('projects/%s/regions/%s/workflowTemplates/%s' %
                      (self.project_id, self.region, self.template_id)),
                body={'instanceId': str(uuid.uuid4())})
            .execute())


class DataprocWorkflowTemplateInstantiateInlineOperator(
        DataprocWorkflowTemplateBaseOperator):
    """
    Instantiate a WorkflowTemplate Inline on Google Cloud Dataproc. The operator will
    wait until the WorkflowTemplate is finished executing.

    .. seealso::
        Please refer to:
        https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/projects.regions.workflowTemplates/instantiateInline

    :param template: The template contents. (templated)
    :type template: map
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
    def __init__(self, template, *args, **kwargs):
        (super()
            .__init__(*args, **kwargs))
        self.template = template

    def start(self):
        self.log.info('Instantiating Inline Template')
        return (
            self.hook.get_conn().projects().regions().workflowTemplates()
            .instantiateInline(
                parent='projects/%s/regions/%s' % (self.project_id, self.region),
                instanceId=str(uuid.uuid4()),
                body=self.template)
            .execute())
