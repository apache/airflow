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

import copy
import os
import six

from airflow.configuration import conf
from airflow.contrib.kubernetes.pod import Pod, Resources
from airflow.contrib.kubernetes.secret import Secret
from airflow.utils.log.logging_mixin import LoggingMixin


class WorkerConfiguration(LoggingMixin):
    """Contains Kubernetes Airflow Worker configuration logic"""

    def __init__(self, kube_config):
        self.kube_config = kube_config
        self.worker_airflow_home = self.kube_config.airflow_home
        self.worker_airflow_dags = self.kube_config.dags_folder
        self.worker_airflow_logs = self.kube_config.base_log_folder
        super(WorkerConfiguration, self).__init__()

    def _get_init_containers(self, volume_mounts):
        """When using git to retrieve the DAGs, use the GitSync Init Container"""
        # If we're using volume claims to mount the dags, no init container is needed
        if self.kube_config.dags_volume_claim:
            return []

        # Otherwise, define a git-sync init container
        init_environment = [{
            'name': 'GIT_SYNC_REPO',
            'value': self.kube_config.git_repo
        }, {
            'name': 'GIT_SYNC_BRANCH',
            'value': self.kube_config.git_branch
        }, {
            'name': 'GIT_SYNC_ROOT',
            'value': os.path.join(
                self.worker_airflow_dags,
                self.kube_config.git_subpath
            )
        }, {
            'name': 'GIT_SYNC_DEST',
            'value': 'dags'
        }, {
            'name': 'GIT_SYNC_ONE_TIME',
            'value': 'true'
        }]
        if self.kube_config.git_user:
            init_environment.append({
                'name': 'GIT_SYNC_USERNAME',
                'value': self.kube_config.git_user
            })
        if self.kube_config.git_password:
            init_environment.append({
                'name': 'GIT_SYNC_PASSWORD',
                'value': self.kube_config.git_password
            })

        volume_mounts[0]['readOnly'] = False
        return [{
            'name': self.kube_config.git_sync_init_container_name,
            'image': self.kube_config.git_sync_container,
            'securityContext': {'runAsUser': 0},
            'env': init_environment,
            'volumeMounts': volume_mounts
        }]

    def _get_environment(self):
        """Defines any necessary environment variables for the pod executor"""
        env = {
            "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        }

        if self.kube_config.airflow_configmap:
            env['AIRFLOW__CORE__AIRFLOW_HOME'] = self.worker_airflow_home
        if self.kube_config.worker_dags_folder:
            env['AIRFLOW__CORE__DAGS_FOLDER'] = self.kube_config.worker_dags_folder
        if (not self.kube_config.airflow_configmap and
                'AIRFLOW__CORE__SQL_ALCHEMY_CONN' not in self.kube_config.kube_secrets):
            env['AIRFLOW__CORE__SQL_ALCHEMY_CONN'] = conf.get("core", "SQL_ALCHEMY_CONN")
        return env

    def _get_secrets(self):
        """Defines any necessary secrets for the pod executor"""
        worker_secrets = []
        for env_var_name, obj_key_pair in six.iteritems(self.kube_config.kube_secrets):
            k8s_secret_obj, k8s_secret_key = obj_key_pair.split('=')
            worker_secrets.append(
                Secret('env', env_var_name, k8s_secret_obj, k8s_secret_key))
        return worker_secrets

    def _get_image_pull_secrets(self):
        """Extracts any image pull secrets for fetching container(s)"""
        if not self.kube_config.image_pull_secrets:
            return []
        return self.kube_config.image_pull_secrets.split(',')

    def init_volumes_and_mounts(self):
        dags_volume_name = 'airflow-dags'
        logs_volume_name = 'airflow-logs'

        def _construct_volume(name, claim):
            volume = {
                'name': name
            }
            if claim:
                volume['persistentVolumeClaim'] = {
                    'claimName': claim
                }
            else:
                volume['emptyDir'] = {}
            return volume

        volumes = [
            _construct_volume(
                dags_volume_name,
                self.kube_config.dags_volume_claim
            ),
            _construct_volume(
                logs_volume_name,
                self.kube_config.logs_volume_claim
            )
        ]

        dag_volume_mount_path = ""

        if self.kube_config.dags_volume_claim:
            dag_volume_mount_path = self.worker_airflow_dags
        else:
            dag_volume_mount_path = os.path.join(
                self.worker_airflow_dags,
                self.kube_config.git_subpath
            )
        dags_volume_mount = {
            'name': dags_volume_name,
            'mountPath': dag_volume_mount_path,
            'readOnly': True,
        }
        if self.kube_config.dags_volume_subpath:
            dags_volume_mount['subPath'] = self.kube_config.dags_volume_subpath

        logs_volume_mount = {
            'name': logs_volume_name,
            'mountPath': self.worker_airflow_logs,
        }
        if self.kube_config.logs_volume_subpath:
            logs_volume_mount['subPath'] = self.kube_config.logs_volume_subpath

        volume_mounts = [
            dags_volume_mount,
            logs_volume_mount
        ]

        # Mount the airflow.cfg file via a configmap the user has specified
        if self.kube_config.airflow_configmap:
            config_volume_name = 'airflow-config'
            config_path = '{}/airflow.cfg'.format(self.worker_airflow_home)
            volumes.append({
                'name': config_volume_name,
                'configMap': {
                    'name': self.kube_config.airflow_configmap
                }
            })
            volume_mounts.append({
                'name': config_volume_name,
                'mountPath': config_path,
                'subPath': 'airflow.cfg',
                'readOnly': True
            })

        return volumes, volume_mounts

    def make_pod(self, namespace, worker_uuid, pod_id, dag_id, task_id, execution_date,
                 airflow_command, kube_executor_config):
        volumes, volume_mounts = self.init_volumes_and_mounts()
        volumes += kube_executor_config.volumes
        volume_mounts += kube_executor_config.volume_mounts
        worker_init_container_spec = self._get_init_containers(
            copy.deepcopy(volume_mounts))
        resources = Resources(
            request_memory=kube_executor_config.request_memory,
            request_cpu=kube_executor_config.request_cpu,
            limit_memory=kube_executor_config.limit_memory,
            limit_cpu=kube_executor_config.limit_cpu
        )
        gcp_sa_key = kube_executor_config.gcp_service_account_key
        annotations = kube_executor_config.annotations.copy()
        if gcp_sa_key:
            annotations['iam.cloud.google.com/service-account'] = gcp_sa_key

        return Pod(
            namespace=namespace,
            name=pod_id,
            image=kube_executor_config.image or self.kube_config.kube_image,
            image_pull_policy=(kube_executor_config.image_pull_policy or
                               self.kube_config.kube_image_pull_policy),
            cmds=airflow_command,
            labels={
                'airflow-worker': worker_uuid,
                'dag_id': dag_id,
                'task_id': task_id,
                'execution_date': execution_date
            },
            envs=self._get_environment(),
            secrets=self._get_secrets(),
            service_account_name=self.kube_config.worker_service_account_name,
            image_pull_secrets=self.kube_config.image_pull_secrets,
            init_containers=worker_init_container_spec,
            volumes=volumes,
            volume_mounts=volume_mounts,
            resources=resources,
            annotations=annotations,
            node_selectors=(kube_executor_config.node_selectors or
                            self.kube_config.kube_node_selectors),
            affinity=kube_executor_config.affinity
        )
