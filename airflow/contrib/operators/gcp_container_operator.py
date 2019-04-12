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
import os
import subprocess
import tempfile

from airflow import AirflowException
from airflow.contrib.hooks.gcp_container_hook import GKEClusterHook
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GKEClusterDeleteOperator(BaseOperator):
    """
    Deletes the cluster, including the Kubernetes endpoint and all worker nodes.

    To delete a certain cluster, you must specify the ``project_id``, the ``name``
    of the cluster, the ``location`` that the cluster is in, and the ``task_id``.

    **Operator Creation**: ::

        operator = GKEClusterDeleteOperator(
                    task_id='cluster_delete',
                    project_id='my-project',
                    location='cluster-location'
                    name='cluster-name')

    .. seealso::
        For more detail about deleting clusters have a look at the reference:
        https://google-cloud-python.readthedocs.io/en/latest/container/gapic/v1/api.html#google.cloud.container_v1.ClusterManagerClient.delete_cluster

    :param project_id: The Google Developers Console [project ID or project number]
    :type project_id: str
    :param name: The name of the resource to delete, in this case cluster name
    :type name: str
    :param location: The name of the Google Compute Engine zone in which the cluster
        resides.
    :type location: str
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: The api version to use
    :type api_version: str
    """
    template_fields = ['project_id', 'gcp_conn_id', 'name', 'location', 'api_version']

    @apply_defaults
    def __init__(self,
                 project_id,
                 name,
                 location,
                 gcp_conn_id='google_cloud_default',
                 api_version='v2',
                 *args,
                 **kwargs):
        super(GKEClusterDeleteOperator, self).__init__(*args, **kwargs)

        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.location = location
        self.api_version = api_version
        self.name = name

    def _check_input(self):
        if not all([self.project_id, self.name, self.location]):
            self.log.error(
                'One of (project_id, name, location) is missing or incorrect')
            raise AirflowException('Operator has incorrect or missing input.')

    def execute(self, context):
        self._check_input()
        hook = GKEClusterHook(self.project_id, self.location)
        delete_result = hook.delete_cluster(name=self.name)
        return delete_result


class GKEClusterCreateOperator(BaseOperator):
    """
    Create a Google Kubernetes Engine Cluster of specified dimensions
    The operator will wait until the cluster is created.

    The **minimum** required to define a cluster to create is:

    ``dict()`` ::
        cluster_def = {'name': 'my-cluster-name',
                       'initial_node_count': 1}

    or

    ``Cluster`` proto ::
        from google.cloud.container_v1.types import Cluster

        cluster_def = Cluster(name='my-cluster-name', initial_node_count=1)

    **Operator Creation**: ::

        operator = GKEClusterCreateOperator(
                    task_id='cluster_create',
                    project_id='my-project',
                    location='my-location'
                    body=cluster_def)

    .. seealso::
        For more detail on about creating clusters have a look at the reference:
        :class:`google.cloud.container_v1.types.Cluster`

    :param project_id: The Google Developers Console [project ID or project number]
    :type project_id: str
    :param location: The name of the Google Compute Engine zone in which the cluster
        resides.
    :type location: str
    :param body: The Cluster definition to create, can be protobuf or python dict, if
        dict it must match protobuf message Cluster
    :type body: dict or google.cloud.container_v1.types.Cluster
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    :param api_version: The api version to use
    :type api_version: str
    """
    template_fields = ['project_id', 'gcp_conn_id', 'location', 'api_version', 'body']

    @apply_defaults
    def __init__(self,
                 project_id,
                 location,
                 body=None,
                 gcp_conn_id='google_cloud_default',
                 api_version='v2',
                 *args,
                 **kwargs):
        super(GKEClusterCreateOperator, self).__init__(*args, **kwargs)

        if body is None:
            body = {}
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.location = location
        self.api_version = api_version
        self.body = body

    def _check_input(self):
        if all([self.project_id, self.location, self.body]):
            if isinstance(self.body, dict) \
                    and 'name' in self.body \
                    and 'initial_node_count' in self.body:
                # Don't throw error
                return
            # If not dict, then must
            elif self.body.name and self.body.initial_node_count:
                return

        self.log.error(
            'One of (project_id, location, body, body[\'name\'], '
            'body[\'initial_node_count\']) is missing or incorrect')
        raise AirflowException('Operator has incorrect or missing input.')

    def execute(self, context):
        self._check_input()
        hook = GKEClusterHook(self.project_id, self.location)
        create_op = hook.create_cluster(cluster=self.body)
        return create_op


KUBE_CONFIG_ENV_VAR = "KUBECONFIG"
G_APP_CRED = "GOOGLE_APPLICATION_CREDENTIALS"


class GKEPodOperator(KubernetesPodOperator):
    """
    Executes a task in a Kubernetes pod in the specified Google Kubernetes
    Engine cluster

    This Operator assumes that the system has gcloud installed and either
    has working default application credentials or has configured a
    connection id with a service account.

    The **minimum** required to define a cluster to create are the variables
    ``task_id``, ``project_id``, ``location``, ``cluster_name``, ``name``,
    ``namespace``, and ``image``

    **Operator Creation**: ::

        operator = GKEPodOperator(task_id='pod_op',
                                  project_id='my-project',
                                  location='us-central1-a',
                                  cluster_name='my-cluster-name',
                                  name='task-name',
                                  namespace='default',
                                  image='perl')

    .. seealso::
        For more detail about application authentication have a look at the reference:
        https://cloud.google.com/docs/authentication/production#providing_credentials_to_your_application

    :param project_id: The Google Developers Console project id
    :type project_id: str
    :param location: The name of the Google Kubernetes Engine zone in which the
        cluster resides, e.g. 'us-central1-a'
    :type location: str
    :param cluster_name: The name of the Google Kubernetes Engine cluster the pod
        should be spawned in
    :type cluster_name: str
    :param gcp_conn_id: The google cloud connection id to use. This allows for
        users to specify a service account.
    :type gcp_conn_id: str
    """
    template_fields = ('project_id', 'location',
                       'cluster_name') + KubernetesPodOperator.template_fields

    @apply_defaults
    def __init__(self,
                 project_id,
                 location,
                 cluster_name,
                 gcp_conn_id='google_cloud_default',
                 *args,
                 **kwargs):
        super(GKEPodOperator, self).__init__(*args, **kwargs)
        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        # Specifying a service account file allows the user to using non default
        # authentication for creating a Kubernetes Pod. This is done by setting the
        # environment variable `GOOGLE_APPLICATION_CREDENTIALS` that gcloud looks at.
        key_file = None

        # If gcp_conn_id is not specified gcloud will use the default
        # service account credentials.
        if self.gcp_conn_id:
            from airflow.hooks.base_hook import BaseHook
            # extras is a deserialized json object
            extras = BaseHook.get_connection(self.gcp_conn_id).extra_dejson
            # key_file only gets set if a json file is created from a JSON string in
            # the web ui, else none
            key_file = self._set_env_from_extras(extras=extras)

        # Write config to a temp file and set the environment variable to point to it.
        # This is to avoid race conditions of reading/writing a single file
        with tempfile.NamedTemporaryFile() as conf_file:
            os.environ[KUBE_CONFIG_ENV_VAR] = conf_file.name
            # Attempt to get/update credentials
            # We call gcloud directly instead of using google-cloud-python api
            # because there is no way to write kubernetes config to a file, which is
            # required by KubernetesPodOperator.
            # The gcloud command looks at the env variable `KUBECONFIG` for where to save
            # the kubernetes config file.
            subprocess.check_call(
                ["gcloud", "container", "clusters", "get-credentials",
                 self.cluster_name,
                 "--zone", self.location,
                 "--project", self.project_id])

            # Since the key file is of type mkstemp() closing the file will delete it from
            # the file system so it cannot be accessed after we don't need it anymore
            if key_file:
                key_file.close()

            # Tell `KubernetesPodOperator` where the config file is located
            self.config_file = os.environ[KUBE_CONFIG_ENV_VAR]
            return super(GKEPodOperator, self).execute(context)

    def _set_env_from_extras(self, extras):
        """
        Sets the environment variable `GOOGLE_APPLICATION_CREDENTIALS` with either:

        - The path to the keyfile from the specified connection id
        - A generated file's path if the user specified JSON in the connection id. The
            file is assumed to be deleted after the process dies due to how mkstemp()
            works.

        The environment variable is used inside the gcloud command to determine correct
        service account to use.
        """
        key_path = self._get_field(extras, 'key_path', False)
        keyfile_json_str = self._get_field(extras, 'keyfile_dict', False)

        if not key_path and not keyfile_json_str:
            self.log.info('Using gcloud with application default credentials.')
        elif key_path:
            os.environ[G_APP_CRED] = key_path
        else:
            # Write service account JSON to secure file for gcloud to reference
            service_key = tempfile.NamedTemporaryFile(delete=False)
            service_key.write(keyfile_json_str)
            os.environ[G_APP_CRED] = service_key.name
            # Return file object to have a pointer to close after use,
            # thus deleting from file system.
            return service_key

    def _get_field(self, extras, field, default=None):
        """
        Fetches a field from extras, and returns it. This is some Airflow
        magic. The google_cloud_platform hook type adds custom UI elements
        to the hook page, which allow admins to specify service_account,
        key_path, etc. They get formatted as shown below.
        """
        long_f = 'extra__google_cloud_platform__{}'.format(field)
        if long_f in extras:
            return extras[long_f]
        else:
            self.log.info('Field %s not found in extras.', field)
            return default
