# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import time
import uuid


from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.version import version
from googleapiclient.errors import HttpError


class DataprocClusterCreateOperator(BaseOperator):
    """
    Create a new cluster on Google Cloud Dataproc. The operator will wait until the
    creation is successful or an error occurs in the creation process.

    The parameters allow to configure the cluster. Please refer to

    https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.clusters

    for a detailed explanation on the different parameters. Most of the configuration
    parameters detailed in the link are available as a parameter to this operator.

    :param cluster_name: The name of the DataProc cluster to create.
    :type cluster_name: string
    :param project_id: The ID of the google cloud project in which
        to create the cluster
    :type project_id: string
    :param num_workers: The # of workers to spin up
    :type num_workers: int
    :param storage_bucket: The storage bucket to use, setting to None lets dataproc
        generate a custom one for you
    :type storage_bucket: string
    :param init_actions_uris: List of GCS uri's containing
        dataproc initialization scripts
    :type init_actions_uris: list[string]
    :param metadata: dict of key-value google compute engine metadata entries
        to add to all instances
    :type metadata: dict
    :param image_version: the version of software inside the Dataproc cluster
    :type image_version: string
    :param properties: dict of properties to set on
        config files (e.g. spark-defaults.conf), see
        https://cloud.google.com/dataproc/docs/reference/rest/v1/ \
        projects.regions.clusters#SoftwareConfig
    :type properties: dict
    :param master_machine_type: Compute engine machine type to use for the master node
    :type master_machine_type: string
    :param master_disk_size: Disk size for the master node
    :type master_disk_size: int
    :param worker_machine_type: Compute engine machine type to use for the worker nodes
    :type worker_machine_type: string
    :param worker_disk_size: Disk size for the worker nodes
    :type worker_disk_size: int
    :param num_preemptible_workers: The # of preemptible worker nodes to spin up
    :type num_preemptible_workers: int
    :param labels: dict of labels to add to the cluster
    :type labels: dict
    :param zone: The zone where the cluster will be located
    :type zone: string
    :param network_uri: The network uri to be used for machine communication, cannot be
        specified with subnetwork_uri
    :type network_uri: string
    :param subnetwork_uri: The subnetwork uri to be used for machine communication,
        cannot be specified with network_uri
    :type subnetwork_uri: string
    :param tags: The GCE tags to add to all instances
    :type tags: list[string]
    :param region: leave as 'global', might become relevant in the future
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    :param service_account: The service account of the dataproc instances.
    :type service_account: string
    :param service_account_scopes: The URIs of service account scopes to be included.
    :type service_account_scopes: list[string]
    """

    template_fields = ['cluster_name', 'project_id', 'zone', 'region']

    @apply_defaults
    def __init__(self,
                 cluster_name,
                 project_id,
                 num_workers,
                 zone,
                 network_uri=None,
                 subnetwork_uri=None,
                 tags=None,
                 storage_bucket=None,
                 init_actions_uris=None,
                 metadata=None,
                 image_version=None,
                 properties=None,
                 master_machine_type='n1-standard-4',
                 master_disk_size=500,
                 worker_machine_type='n1-standard-4',
                 worker_disk_size=500,
                 num_preemptible_workers=0,
                 labels=None,
                 region='global',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 service_account=None,
                 service_account_scopes=None,
                 *args,
                 **kwargs):

        super(DataprocClusterCreateOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.cluster_name = cluster_name
        self.project_id = project_id
        self.num_workers = num_workers
        self.num_preemptible_workers = num_preemptible_workers
        self.storage_bucket = storage_bucket
        self.init_actions_uris = init_actions_uris
        self.metadata = metadata
        self.image_version = image_version
        self.properties = properties
        self.master_machine_type = master_machine_type
        self.master_disk_size = master_disk_size
        self.worker_machine_type = worker_machine_type
        self.worker_disk_size = worker_disk_size
        self.labels = labels
        self.zone = zone
        self.network_uri = network_uri
        self.subnetwork_uri = subnetwork_uri
        self.tags = tags
        self.region = region
        self.service_account = service_account
        self.service_account_scopes = service_account_scopes

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

    def _build_cluster_data(self):
        zone_uri = \
            'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(
                self.project_id, self.zone
            )
        master_type_uri = \
            "https://www.googleapis.com/compute/v1/projects/{}/zones/{}/machineTypes/{}".format(
                self.project_id, self.zone, self.master_machine_type
            )
        worker_type_uri = \
            "https://www.googleapis.com/compute/v1/projects/{}/zones/{}/machineTypes/{}".format(
                self.project_id, self.zone, self.worker_machine_type
            )
        cluster_data = {
            'projectId': self.project_id,
            'clusterName': self.cluster_name,
            'config': {
                'gceClusterConfig': {
                    'zoneUri': zone_uri
                },
                'masterConfig': {
                    'numInstances': 1,
                    'machineTypeUri': master_type_uri,
                    'diskConfig': {
                        'bootDiskSizeGb': self.master_disk_size
                    }
                },
                'workerConfig': {
                    'numInstances': self.num_workers,
                    'machineTypeUri': worker_type_uri,
                    'diskConfig': {
                        'bootDiskSizeGb': self.worker_disk_size
                    }
                },
                'secondaryWorkerConfig': {},
                'softwareConfig': {}
            }
        }
        if self.num_preemptible_workers > 0:
            cluster_data['config']['secondaryWorkerConfig'] = {
                'numInstances': self.num_preemptible_workers,
                'machineTypeUri': worker_type_uri,
                'diskConfig': {
                    'bootDiskSizeGb': self.worker_disk_size
                },
                'isPreemptible': True
            }

        cluster_data['labels'] = self.labels if self.labels else {}
        # Dataproc labels must conform to the following regex:
        # [a-z]([-a-z0-9]*[a-z0-9])? (current airflow version string follows
        # semantic versioning spec: x.y.z).
        cluster_data['labels'].update({'airflow-version':
                                       'v' + version.replace('.', '-').replace('+','-')})
        if self.storage_bucket:
            cluster_data['config']['configBucket'] = self.storage_bucket
        if self.metadata:
            cluster_data['config']['gceClusterConfig']['metadata'] = self.metadata
        if self.network_uri:
            cluster_data['config']['gceClusterConfig']['networkUri'] = self.network_uri
        if self.subnetwork_uri:
            cluster_data['config']['gceClusterConfig']['subnetworkUri'] = self.subnetwork_uri
        if self.tags:
            cluster_data['config']['gceClusterConfig']['tags'] = self.tags
        if self.image_version:
            cluster_data['config']['softwareConfig']['imageVersion'] = self.image_version
        if self.properties:
            cluster_data['config']['softwareConfig']['properties'] = self.properties
        if self.init_actions_uris:
            init_actions_dict = [
                {'executableFile': uri} for uri in self.init_actions_uris
            ]
            cluster_data['config']['initializationActions'] = init_actions_dict
        if self.service_account:
            cluster_data['config']['gceClusterConfig']['serviceAccount'] =\
                    self.service_account
        if self.service_account_scopes:
            cluster_data['config']['gceClusterConfig']['serviceAccountScopes'] =\
                    self.service_account_scopes
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


class DataprocClusterDeleteOperator(BaseOperator):
    """
    Delete a cluster on Google Cloud Dataproc. The operator will wait until the
    cluster is destroyed.

    :param cluster_name: The name of the cluster to create.
    :type cluster_name: string
    :param project_id: The ID of the google cloud project in which
        the cluster runs
    :type project_id: string
    :param region: leave as 'global', might become relevant in the future
    :type region: string
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
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

        super(DataprocClusterDeleteOperator, self).__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.cluster_name = cluster_name
        self.project_id = project_id
        self.region = region

    def _wait_for_done(self, service, operation_name):
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

    :param query: The query or reference to the query file (pg or pig extension).
    :type query: string
    :param query_uri: The uri of a pig script on Cloud Storage.
    :type query_uri: string
    :param variables: Map of named parameters for the query.
    :type variables: dict
    :param job_name: The job name used in the DataProc cluster. This name by default
        is the task_id appended with the execution data, but can be templated. The
        name will always be appended with a random number to avoid name clashes.
    :type job_name: string
    :param cluster_name: The name of the DataProc cluster.
    :type cluster_name: string
    :param dataproc_pig_properties: Map for the Pig properties. Ideal to put in
        default arguments
    :type dataproc_pig_properties: dict
    :param dataproc_pig_jars: URIs to jars provisioned in Cloud Storage (example: for
        UDFs and libs) and are ideal to put in default arguments.
    :type dataproc_pig_jars: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    :param region: The specified region where the dataproc cluster is created.
    :type region: string
    """
    template_fields = ['query', 'variables', 'job_name', 'cluster_name']
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
            *args,
            **kwargs):

        super(DataProcPigOperator, self).__init__(*args, **kwargs)
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

        hook.submit(hook.project_id, job.build(), self.region)


class DataProcHiveOperator(BaseOperator):
    """
    Start a Hive query Job on a Cloud DataProc cluster.

    :param query: The query or reference to the query file (q extension).
    :type query: string
    :param query_uri: The uri of a hive script on Cloud Storage.
    :type query_uri: string
    :param variables: Map of named parameters for the query.
    :type variables: dict
    :param job_name: The job name used in the DataProc cluster. This name by default
        is the task_id appended with the execution data, but can be templated. The
        name will always be appended with a random number to avoid name clashes.
    :type job_name: string
    :param cluster_name: The name of the DataProc cluster.
    :type cluster_name: string
    :param dataproc_hive_properties: Map for the Pig properties. Ideal to put in
        default arguments
    :type dataproc_hive_properties: dict
    :param dataproc_hive_jars: URIs to jars provisioned in Cloud Storage (example: for
        UDFs and libs) and are ideal to put in default arguments.
    :type dataproc_hive_jars: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    :param region: The specified region where the dataproc cluster is created.
    :type region: string
    """
    template_fields = ['query', 'variables', 'job_name', 'cluster_name']
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
            *args,
            **kwargs):

        super(DataProcHiveOperator, self).__init__(*args, **kwargs)
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

        hook.submit(hook.project_id, job.build(), self.region)


class DataProcSparkSqlOperator(BaseOperator):
    """
    Start a Spark SQL query Job on a Cloud DataProc cluster.

    :param query: The query or reference to the query file (q extension).
    :type query: string
    :param query_uri: The uri of a spark sql script on Cloud Storage.
    :type query_uri: string
    :param variables: Map of named parameters for the query.
    :type variables: dict
    :param job_name: The job name used in the DataProc cluster. This name by default
        is the task_id appended with the execution data, but can be templated. The
        name will always be appended with a random number to avoid name clashes.
    :type job_name: string
    :param cluster_name: The name of the DataProc cluster.
    :type cluster_name: string
    :param dataproc_spark_properties: Map for the Pig properties. Ideal to put in
        default arguments
    :type dataproc_spark_properties: dict
    :param dataproc_spark_jars: URIs to jars provisioned in Cloud Storage (example:
        for UDFs and libs) and are ideal to put in default arguments.
    :type dataproc_spark_jars: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    :param region: The specified region where the dataproc cluster is created.
    :type region: string
    """
    template_fields = ['query', 'variables', 'job_name', 'cluster_name']
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
            *args,
            **kwargs):

        super(DataProcSparkSqlOperator, self).__init__(*args, **kwargs)
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

        hook.submit(hook.project_id, job.build(), self.region)


class DataProcSparkOperator(BaseOperator):
    """
    Start a Spark Job on a Cloud DataProc cluster.

    :param main_jar: URI of the job jar provisioned on Cloud Storage. (use this or
            the main_class, not both together).
    :type main_jar: string
    :param main_class: Name of the job class. (use this or the main_jar, not both
        together).
    :type main_class: string
    :param arguments: Arguments for the job.
    :type arguments: list
    :param archives: List of archived files that will be unpacked in the work
        directory. Should be stored in Cloud Storage.
    :type archives: list
    :param files: List of files to be copied to the working directory
    :type files: list
    :param job_name: The job name used in the DataProc cluster. This name by default
        is the task_id appended with the execution data, but can be templated. The
        name will always be appended with a random number to avoid name clashes.
    :type job_name: string
    :param cluster_name: The name of the DataProc cluster.
    :type cluster_name: string
    :param dataproc_spark_properties: Map for the Pig properties. Ideal to put in
        default arguments
    :type dataproc_spark_properties: dict
    :param dataproc_spark_jars: URIs to jars provisioned in Cloud Storage (example:
        for UDFs and libs) and are ideal to put in default arguments.
    :type dataproc_spark_jars: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    :param region: The specified region where the dataproc cluster is created.
    :type region: string
    """

    template_fields = ['arguments', 'job_name', 'cluster_name']
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
            *args,
            **kwargs):

        super(DataProcSparkOperator, self).__init__(*args, **kwargs)
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

        hook.submit(hook.project_id, job.build(), self.region)


class DataProcHadoopOperator(BaseOperator):
    """
    Start a Hadoop Job on a Cloud DataProc cluster.

    :param main_jar: URI of the job jar provisioned on Cloud Storage. (use this or
            the main_class, not both together).
    :type main_jar: string
    :param main_class: Name of the job class. (use this or the main_jar, not both
        together).
    :type main_class: string
    :param arguments: Arguments for the job.
    :type arguments: list
    :param archives: List of archived files that will be unpacked in the work
        directory. Should be stored in Cloud Storage.
    :type archives: list
    :param files: List of files to be copied to the working directory
    :type files: list
    :param job_name: The job name used in the DataProc cluster. This name by default
        is the task_id appended with the execution data, but can be templated. The
        name will always be appended with a random number to avoid name clashes.
    :type job_name: string
    :param cluster_name: The name of the DataProc cluster.
    :type cluster_name: string
    :param dataproc_hadoop_properties: Map for the Pig properties. Ideal to put in
        default arguments
    :type dataproc_hadoop_properties: dict
    :param dataproc_hadoop_jars: URIs to jars provisioned in Cloud Storage (example:
        for UDFs and libs) and are ideal to put in default arguments.
    :type dataproc_hadoop_jars: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    :param region: The specified region where the dataproc cluster is created.
    :type region: string
    """

    template_fields = ['arguments', 'job_name', 'cluster_name']
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
            *args,
            **kwargs):

        super(DataProcHadoopOperator, self).__init__(*args, **kwargs)
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

        hook.submit(hook.project_id, job.build(), self.region)


class DataProcPySparkOperator(BaseOperator):
    """
    Start a PySpark Job on a Cloud DataProc cluster.

    :param main: [Required] The Hadoop Compatible Filesystem (HCFS) URI of the main
            Python file to use as the driver. Must be a .py file.
    :type main: string
    :param arguments: Arguments for the job.
    :type arguments: list
    :param archives: List of archived files that will be unpacked in the work
        directory. Should be stored in Cloud Storage.
    :type archives: list
    :param files: List of files to be copied to the working directory
    :type files: list
    :param pyfiles: List of Python files to pass to the PySpark framework.
        Supported file types: .py, .egg, and .zip
    :type pyfiles: list
    :param job_name: The job name used in the DataProc cluster. This name by default
        is the task_id appended with the execution data, but can be templated. The
        name will always be appended with a random number to avoid name clashes.
    :type job_name: string
    :param cluster_name: The name of the DataProc cluster.
    :type cluster_name: string
    :param dataproc_pyspark_properties: Map for the Pig properties. Ideal to put in
        default arguments
    :type dataproc_pyspark_properties: dict
    :param dataproc_pyspark_jars: URIs to jars provisioned in Cloud Storage (example:
        for UDFs and libs) and are ideal to put in default arguments.
    :type dataproc_pyspark_jars: list
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: string
    :param region: The specified region where the dataproc cluster is created.
    :type region: string
    """

    template_fields = ['arguments', 'job_name', 'cluster_name']
    ui_color = '#0273d4'

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
            *args,
            **kwargs):

        super(DataProcPySparkOperator, self).__init__(*args, **kwargs)
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

    def execute(self, context):
        hook = DataProcHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)
        job = hook.create_job_template(self.task_id, self.cluster_name, "pysparkJob",
                                       self.dataproc_properties)

        job.set_python_main(self.main)
        job.add_args(self.arguments)
        job.add_jar_file_uris(self.dataproc_jars)
        job.add_archive_uris(self.archives)
        job.add_file_uris(self.files)
        job.add_python_file_uris(self.pyfiles)
        job.set_job_name(self.job_name)

        hook.submit(hook.project_id, job.build(), self.region)


class DataprocWorkflowTemplateBaseOperator(BaseOperator):
    template_fields = ['template_id', 'template']

    @apply_defaults
    def __init__(self,
                 project_id,
                 region='global',
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(DataprocWorkflowTemplateBaseOperator, self).__init__(*args, **kwargs)
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
        self.hook.await(self.start())

    def start(self, context):
        raise AirflowException('plese start a workflow operation')


class DataprocWorkflowTemplateInstantiateOperator(DataprocWorkflowTemplateBaseOperator):
    """
    Instantiate a WorkflowTemplate on Google Cloud Dataproc. The operator will wait
    until the WorkflowTemplate is finished executing.

    .. seealso::
        Please refer to:
        https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/projects.regions.workflowTemplates/instantiate

    :param template_id: The id of the template.
    :type template_id: string
    :param project_id: The ID of the google cloud project in which
        the template runs
    :type project_id: string
    :param region: leave as 'global', might become relevant in the future
    :type region: string
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
        :type delegate_to: string
    """

    @apply_defaults
    def __init__(self, template_id, *args, **kwargs):
        (super(DataprocWorkflowTemplateInstantiateOperator, self)
            .__init__(*args, **kwargs))
        self.template_id = template_id

    def start(self):
        self.log.info('Instantiating Template: %s', self.template_id)
        return (
            self.hook.get_conn().projects().regions().workflowTemplates()
            .instantiate(
                name=('projects/%s/regions/%s/workflowTemplates/%s' %
                      (self.project_id, self.region, self.template_id)),
                body={'instanceId': str(uuid.uuid1())})
            .execute())


class DataprocWorkflowTemplateInstantiateInlineOperator(
        DataprocWorkflowTemplateBaseOperator):
    """
    Instantiate a WorkflowTemplate Inline on Google Cloud Dataproc. The operator will
    wait until the WorkflowTemplate is finished executing.

    .. seealso::
        Please refer to:
        https://cloud.google.com/dataproc/docs/reference/rest/v1beta2/projects.regions.workflowTemplates/instantiateInline

    :param template: The template contents.
    :type template: map
    :param project_id: The ID of the google cloud project in which
        the template runs
    :type project_id: string
    :param region: leave as 'global', might become relevant in the future
    :type region: string
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    """

    @apply_defaults
    def __init__(self, template, *args, **kwargs):
        (super(DataprocWorkflowTemplateInstantiateInlineOperator, self)
            .__init__(*args, **kwargs))
        self.template = template

    def start(self):
        self.log.info('Instantiating Inline Template')
        return (
            self.hook.get_conn().projects().regions().workflowTemplates()
            .instantiateInline(
                parent='projects/%s/regions/%s' % (self.project_id, self.region),
                instanceId=str(uuid.uuid1()),
                body=self.template)
            .execute())
