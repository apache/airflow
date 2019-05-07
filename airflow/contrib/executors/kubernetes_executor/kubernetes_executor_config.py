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

from airflow.configuration import conf
from airflow import configuration, settings
from airflow.exceptions import AirflowConfigException
from airflow.executors import Executors
import json


class KubeConfig:
    core_section = 'core'
    kubernetes_section = 'kubernetes'

    def __init__(self):
        configuration_dict = configuration.as_dict(display_sensitive=True)
        self.core_configuration = configuration_dict['core']
        self.kube_secrets = configuration_dict.get('kubernetes_secrets', {})
        self.kube_env_vars = configuration_dict.get('kubernetes_environment_variables', {})
        self.env_from_configmap_ref = configuration.get(self.kubernetes_section,
                                                        'env_from_configmap_ref')
        self.env_from_secret_ref = configuration.get(self.kubernetes_section,
                                                     'env_from_secret_ref')
        self.airflow_home = settings.AIRFLOW_HOME
        self.dags_folder = configuration.get(self.core_section, 'dags_folder')
        self.parallelism = configuration.getint(self.core_section, 'PARALLELISM')
        self.worker_container_repository = configuration.get(
            self.kubernetes_section, 'worker_container_repository')
        self.worker_container_tag = configuration.get(
            self.kubernetes_section, 'worker_container_tag')
        self.kube_image = '{}:{}'.format(
            self.worker_container_repository, self.worker_container_tag)
        self.kube_image_pull_policy = configuration.get(
            self.kubernetes_section, "worker_container_image_pull_policy"
        )
        self.kube_node_selectors = configuration_dict.get('kubernetes_node_selectors', {})
        self.kube_annotations = configuration_dict.get('kubernetes_annotations', {})
        self.kube_labels = configuration_dict.get('kubernetes_labels', {})
        self.delete_worker_pods = conf.getboolean(
            self.kubernetes_section, 'delete_worker_pods')
        self.worker_pods_creation_batch_size = conf.getint(
            self.kubernetes_section, 'worker_pods_creation_batch_size')
        self.worker_service_account_name = conf.get(
            self.kubernetes_section, 'worker_service_account_name')
        self.image_pull_secrets = conf.get(self.kubernetes_section, 'image_pull_secrets')

        # NOTE: user can build the dags into the docker image directly,
        # this will set to True if so
        self.dags_in_image = conf.getboolean(self.kubernetes_section, 'dags_in_image')

        # Run as user for pod security context
        self.worker_run_as_user = conf.get(self.kubernetes_section, 'run_as_user')
        self.worker_fs_group = conf.get(self.kubernetes_section, 'fs_group')

        # NOTE: `git_repo` and `git_branch` must be specified together as a pair
        # The http URL of the git repository to clone from
        self.git_repo = conf.get(self.kubernetes_section, 'git_repo')
        # The branch of the repository to be checked out
        self.git_branch = conf.get(self.kubernetes_section, 'git_branch')
        # Optionally, the directory in the git repository containing the dags
        self.git_subpath = conf.get(self.kubernetes_section, 'git_subpath')
        # Optionally, the root directory for git operations
        self.git_sync_root = conf.get(self.kubernetes_section, 'git_sync_root')
        # Optionally, the name at which to publish the checked-out files under --root
        self.git_sync_dest = conf.get(self.kubernetes_section, 'git_sync_dest')
        # Optionally, if git_dags_folder_mount_point is set the worker will use
        # {git_dags_folder_mount_point}/{git_sync_dest}/{git_subpath} as dags_folder
        self.git_dags_folder_mount_point = conf.get(self.kubernetes_section,
                                                    'git_dags_folder_mount_point')

        # Optionally a user may supply a (`git_user` AND `git_password`) OR
        # (`git_ssh_key_secret_name` AND `git_ssh_key_secret_key`) for private repositories
        self.git_user = conf.get(self.kubernetes_section, 'git_user')
        self.git_password = conf.get(self.kubernetes_section, 'git_password')
        self.git_ssh_key_secret_name = conf.get(self.kubernetes_section, 'git_ssh_key_secret_name')
        self.git_ssh_known_hosts_configmap_name = conf.get(self.kubernetes_section,
                                                           'git_ssh_known_hosts_configmap_name')

        # NOTE: The user may optionally use a volume claim to mount a PV containing
        # DAGs directly
        self.dags_volume_claim = conf.get(self.kubernetes_section, 'dags_volume_claim')

        # This prop may optionally be set for PV Claims and is used to write logs
        self.logs_volume_claim = conf.get(self.kubernetes_section, 'logs_volume_claim')

        # This prop may optionally be set for PV Claims and is used to locate DAGs
        # on a SubPath
        self.dags_volume_subpath = conf.get(
            self.kubernetes_section, 'dags_volume_subpath')

        # This prop may optionally be set for PV Claims and is used to locate logs
        # on a SubPath
        self.logs_volume_subpath = conf.get(
            self.kubernetes_section, 'logs_volume_subpath')

        # Optionally, hostPath volume containing DAGs
        self.dags_volume_host = conf.get(self.kubernetes_section, 'dags_volume_host')

        # Optionally, write logs to a hostPath Volume
        self.logs_volume_host = conf.get(self.kubernetes_section, 'logs_volume_host')

        # This prop may optionally be set for PV Claims and is used to write logs
        self.base_log_folder = configuration.get(self.core_section, 'base_log_folder')

        # The Kubernetes Namespace in which the Scheduler and Webserver reside. Note
        # that if your
        # cluster has RBAC enabled, your scheduler may need service account permissions to
        # create, watch, get, and delete pods in this namespace.
        self.kube_namespace = conf.get(self.kubernetes_section, 'namespace')
        # The Kubernetes Namespace in which pods will be created by the executor. Note
        # that if your
        # cluster has RBAC enabled, your workers may need service account permissions to
        # interact with cluster components.
        self.executor_namespace = conf.get(self.kubernetes_section, 'namespace')
        # Task secrets managed by KubernetesExecutor.
        self.gcp_service_account_keys = conf.get(self.kubernetes_section,
                                                 'gcp_service_account_keys')

        # If the user is using the git-sync container to clone their repository via git,
        # allow them to specify repository, tag, and pod name for the init container.
        self.git_sync_container_repository = conf.get(
            self.kubernetes_section, 'git_sync_container_repository')

        self.git_sync_container_tag = conf.get(
            self.kubernetes_section, 'git_sync_container_tag')
        self.git_sync_container = '{}:{}'.format(
            self.git_sync_container_repository, self.git_sync_container_tag)

        self.git_sync_init_container_name = conf.get(
            self.kubernetes_section, 'git_sync_init_container_name')

        # The worker pod may optionally have a  valid Airflow config loaded via a
        # configmap
        self.airflow_configmap = conf.get(self.kubernetes_section, 'airflow_configmap')

        affinity_json = conf.get(self.kubernetes_section, 'affinity')
        if affinity_json:
            self.kube_affinity = json.loads(affinity_json)
        else:
            self.kube_affinity = None

        tolerations_json = conf.get(self.kubernetes_section, 'tolerations')
        if tolerations_json:
            self.kube_tolerations = json.loads(tolerations_json)
        else:
            self.kube_tolerations = None

        kube_client_request_args = conf.get(self.kubernetes_section, 'kube_client_request_args')
        if kube_client_request_args:
            self.kube_client_request_args = json.loads(kube_client_request_args)
            if self.kube_client_request_args['_request_timeout'] and \
                    isinstance(self.kube_client_request_args['_request_timeout'], list):
                self.kube_client_request_args['_request_timeout'] = \
                    tuple(self.kube_client_request_args['_request_timeout'])
        else:
            self.kube_client_request_args = {}
        self._validate()

    def _validate(self):
        # TODO: use XOR for dags_volume_claim and git_dags_folder_mount_point
        if not self.dags_volume_claim \
            and not self.dags_volume_host \
            and not self.dags_in_image \
            and (not self.git_repo or not self.git_branch or not self.git_dags_folder_mount_point):
            raise AirflowConfigException(
                'In kubernetes mode the following must be set in the `kubernetes` '
                'config section: `dags_volume_claim` '
                'or `dags_volume_host` '
                'or `dags_in_image` '
                'or `git_repo and git_branch and git_dags_folder_mount_point`')
        if self.git_repo \
            and (self.git_user or self.git_password) \
            and self.git_ssh_key_secret_name:
            raise AirflowConfigException(
                'In kubernetes mode, using `git_repo` to pull the DAGs: '
                'for private repositories, either `git_user` and `git_password` '
                'must be set for authentication through user credentials; '
                'or `git_ssh_key_secret_name` must be set for authentication '
                'through ssh key, but not both')


class KubernetesExecutorConfig:
    def __init__(self, image=None, image_pull_policy=None, request_memory=None,
                 request_cpu=None, limit_memory=None, limit_cpu=None,
                 gcp_service_account_key=None, node_selectors=None, affinity=None,
                 annotations=None, volumes=None, volume_mounts=None, tolerations=None):
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.request_memory = request_memory
        self.request_cpu = request_cpu
        self.limit_memory = limit_memory
        self.limit_cpu = limit_cpu
        self.gcp_service_account_key = gcp_service_account_key
        self.node_selectors = node_selectors
        self.affinity = affinity
        self.annotations = annotations
        self.volumes = volumes
        self.volume_mounts = volume_mounts
        self.tolerations = tolerations

    def __repr__(self):
        return "{}(image={}, image_pull_policy={}, request_memory={}, request_cpu={}, " \
               "limit_memory={}, limit_cpu={}, gcp_service_account_key={}, " \
               "node_selectors={}, affinity={}, annotations={}, volumes={}, " \
               "volume_mounts={}, tolerations={})" \
            .format(KubernetesExecutorConfig.__name__, self.image, self.image_pull_policy,
                    self.request_memory, self.request_cpu, self.limit_memory,
                    self.limit_cpu, self.gcp_service_account_key, self.node_selectors,
                    self.affinity, self.annotations, self.volumes, self.volume_mounts,
                    self.tolerations)

    @staticmethod
    def from_dict(obj):
        if obj is None:
            return KubernetesExecutorConfig()

        if not isinstance(obj, dict):
            raise TypeError(
                'Cannot convert a non-dictionary object into a KubernetesExecutorConfig')

        namespaced = obj.get(Executors.KubernetesExecutor, {})

        return KubernetesExecutorConfig(
            image=namespaced.get('image', None),
            image_pull_policy=namespaced.get('image_pull_policy', None),
            request_memory=namespaced.get('request_memory', None),
            request_cpu=namespaced.get('request_cpu', None),
            limit_memory=namespaced.get('limit_memory', None),
            limit_cpu=namespaced.get('limit_cpu', None),
            gcp_service_account_key=namespaced.get('gcp_service_account_key', None),
            node_selectors=namespaced.get('node_selectors', None),
            affinity=namespaced.get('affinity', None),
            annotations=namespaced.get('annotations', {}),
            volumes=namespaced.get('volumes', []),
            volume_mounts=namespaced.get('volume_mounts', []),
            tolerations=namespaced.get('tolerations', None),
        )

    def as_dict(self):
        return {
            'image': self.image,
            'image_pull_policy': self.image_pull_policy,
            'request_memory': self.request_memory,
            'request_cpu': self.request_cpu,
            'limit_memory': self.limit_memory,
            'limit_cpu': self.limit_cpu,
            'gcp_service_account_key': self.gcp_service_account_key,
            'node_selectors': self.node_selectors,
            'affinity': self.affinity,
            'annotations': self.annotations,
            'volumes': self.volumes,
            'volume_mounts': self.volume_mounts,
            'tolerations': self.tolerations,
        }
