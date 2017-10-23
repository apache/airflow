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

import copy
import os
import six

from airflow.contrib.kubernetes.pod import Pod
from airflow.contrib.kubernetes.secret import Secret


class WorkerConfiguration:
    """Contains Kubernetes Airflow Worker configuration logic"""

    def __init__(self, kube_config):
        self.kube_config = kube_config

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
                'value': '/tmp'
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

    def _get_volumes_and_mounts(self):
        """Determine volumes and volume mounts for Airflow workers"""
        dags_volume_name = "airflow-dags"
        dags_path = os.path.join(self.kube_config.dags_folder, self.kube_config.git_subpath)
        volumes = [{
            'name': dags_volume_name
        }]
        volume_mounts = [{
            'name': dags_volume_name,
            'mountPath': dags_path,
            'readOnly': True
        }]

        # Mount the airflow.cfg file via a configmap the user has specified
        if self.kube_config.airflow_configmap:
            config_volume_name = "airflow-config"
            config_path = '{}/airflow.cfg'.format(self.kube_config.airflow_home)
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

        # A PV with the DAGs should be mounted
        if self.kube_config.dags_volume_claim:
            volumes[0]['persistentVolumeClaim'] = {"claimName": self.kube_config.dags_volume_claim}
            if self.kube_config.dags_volume_subpath:
                volume_mounts[0]["subPath"] = self.kube_config.dags_volume_subpath
        else:
            # Create a Shared Volume for the Git-Sync module to populate
            volumes[0]["emptyDir"] = {}
        return volumes, volume_mounts

    def _get_environment(self):
        """Defines any necessary environment variables for the pod executor"""
        env = {
            'AIRFLOW__CORE__DAGS_FOLDER': '/tmp/dags',
            'AIRFLOW__CORE__EXECUTOR': 'LocalExecutor'
        }
        if self.kube_config.airflow_configmap:
            env['AIRFLOW__CORE__AIRFLOW_HOME'] = self.kube_config.airflow_home
        return env

    def _get_secrets(self):
        """Defines any necessary secrets for the pod executor"""
        worker_secrets = []
        for env_var_name, obj_key_pair in six.iteritems(self.kube_config.kube_secrets):
            k8s_secret_obj, k8s_secret_key = obj_key_pair.split('=')
            worker_secrets.append(Secret('env', env_var_name, k8s_secret_obj, k8s_secret_key))
        return worker_secrets

    def _get_image_pull_secrets(self):
        """Extracts any image pull secrets for fetching container(s)"""
        if not self.kube_config.image_pull_secrets:
            return []
        return self.kube_config.image_pull_secrets.split(',')

    def make_pod(self, namespace, pod_id, dag_id, task_id, execution_date, airflow_command):
        volumes, volume_mounts = self._get_volumes_and_mounts()
        worker_init_container_spec = self._get_init_containers(copy.deepcopy(volume_mounts))
        return Pod(
            namespace=namespace,
            name=pod_id,
            image=self.kube_config.kube_image,
            cmds=["bash", "-cx", "--"],
            args=[airflow_command],
            labels={
                "airflow-slave": "",
                "dag_id": dag_id,
                "task_id": task_id,
                "execution_date": execution_date
            },
            envs=self._get_environment(),
            secrets=self._get_secrets(),
            service_account_name=self.kube_config.worker_service_account_name,
            image_pull_secrets=self.kube_config.image_pull_secrets,
            init_containers=worker_init_container_spec,
            volumes=volumes,
            volume_mounts=volume_mounts
        )
