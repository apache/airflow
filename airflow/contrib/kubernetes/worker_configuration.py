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

import os
import six

from airflow.configuration import conf
from airflow.contrib.kubernetes.pod import Pod, Resources
from airflow.contrib.kubernetes.secret import Secret
from airflow.utils.log.logging_mixin import LoggingMixin


class WorkerConfiguration(LoggingMixin):
    """Contains Kubernetes Airflow Worker configuration logic"""

    dags_volume_name = 'airflow-dags'
    logs_volume_name = 'airflow-logs'
    git_sync_ssh_secret_volume_name = 'git-sync-ssh-key'
    git_ssh_key_secret_key = 'gitSshKey'
    git_sync_ssh_known_hosts_volume_name = 'git-sync-known-hosts'
    git_ssh_known_hosts_configmap_key = 'known_hosts'

    def __init__(self, kube_config):
        self.kube_config = kube_config
        self.worker_airflow_home = self.kube_config.airflow_home
        self.worker_airflow_dags = self.kube_config.dags_folder
        self.worker_airflow_logs = self.kube_config.base_log_folder

        super().__init__()

    def _get_init_containers(self):
        """When using git to retrieve the DAGs, use the GitSync Init Container"""
        # If we're using volume claims to mount the dags, no init container is needed
        if self.kube_config.dags_volume_claim or \
           self.kube_config.dags_volume_host or self.kube_config.dags_in_image:
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
            'value': self.kube_config.git_sync_root
        }, {
            'name': 'GIT_SYNC_DEST',
            'value': self.kube_config.git_sync_dest
        }, {
            'name': 'GIT_SYNC_DEPTH',
            'value': '1'
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

        volume_mounts = [{
            'mountPath': self.kube_config.git_sync_root,
            'name': self.dags_volume_name,
            'readOnly': False
        }]
        if self.kube_config.git_ssh_key_secret_name:
            volume_mounts.append({
                'name': self.git_sync_ssh_secret_volume_name,
                'mountPath': '/etc/git-secret/ssh',
                'subPath': 'ssh'
            })
            init_environment.extend([
                {
                    'name': 'GIT_SSH_KEY_FILE',
                    'value': '/etc/git-secret/ssh'
                },
                {
                    'name': 'GIT_SYNC_SSH',
                    'value': 'true'
                }])
        if self.kube_config.git_ssh_known_hosts_configmap_name:
            volume_mounts.append({
                'name': self.git_sync_ssh_known_hosts_volume_name,
                'mountPath': '/etc/git-secret/known_hosts',
                'subPath': 'known_hosts'
            })
            init_environment.extend([
                {
                    'name': 'GIT_KNOWN_HOSTS',
                    'value': 'true'
                },
                {
                    'name': 'GIT_SSH_KNOWN_HOSTS_FILE',
                    'value': '/etc/git-secret/known_hosts'
                }
            ])
        else:
            init_environment.append({
                'name': 'GIT_KNOWN_HOSTS',
                'value': 'false'
            })

        return [{
            'name': self.kube_config.git_sync_init_container_name,
            'image': self.kube_config.git_sync_container,
            'securityContext': {'runAsUser': 65533},  # git-sync user
            'env': init_environment,
            'volumeMounts': volume_mounts
        }]

    def _get_environment(self):
        """Defines any necessary environment variables for the pod executor"""
        env = {}

        for env_var_name, env_var_val in six.iteritems(self.kube_config.kube_env_vars):
            env[env_var_name] = env_var_val

        env["AIRFLOW__CORE__EXECUTOR"] = "LocalExecutor"

        if self.kube_config.airflow_configmap:
            env['AIRFLOW_HOME'] = self.worker_airflow_home
            env['AIRFLOW__CORE__DAGS_FOLDER'] = self.worker_airflow_dags
        if (not self.kube_config.airflow_configmap and
                'AIRFLOW__CORE__SQL_ALCHEMY_CONN' not in self.kube_config.kube_secrets):
            env['AIRFLOW__CORE__SQL_ALCHEMY_CONN'] = conf.get("core", "SQL_ALCHEMY_CONN")
        if self.kube_config.git_dags_folder_mount_point:
            # /root/airflow/dags/repo/dags
            dag_volume_mount_path = os.path.join(
                self.kube_config.git_dags_folder_mount_point,
                self.kube_config.git_sync_dest,  # repo
                self.kube_config.git_subpath     # dags
            )
            env['AIRFLOW__CORE__DAGS_FOLDER'] = dag_volume_mount_path
        return env

    def _get_configmaps(self):
        """Extracts any configmapRefs to envFrom"""
        if not self.kube_config.env_from_configmap_ref:
            return []
        return self.kube_config.env_from_configmap_ref.split(',')

    def _get_secrets(self):
        """Defines any necessary secrets for the pod executor"""
        worker_secrets = []

        for env_var_name, obj_key_pair in six.iteritems(self.kube_config.kube_secrets):
            k8s_secret_obj, k8s_secret_key = obj_key_pair.split('=')
            worker_secrets.append(
                Secret('env', env_var_name, k8s_secret_obj, k8s_secret_key)
            )

        if self.kube_config.env_from_secret_ref:
            for secret_ref in self.kube_config.env_from_secret_ref.split(','):
                worker_secrets.append(
                    Secret('env', None, secret_ref)
                )

        return worker_secrets

    def _get_image_pull_secrets(self):
        """Extracts any image pull secrets for fetching container(s)"""
        if not self.kube_config.image_pull_secrets:
            return []
        return self.kube_config.image_pull_secrets.split(',')

    def _get_security_context(self):
        """Defines the security context"""
        security_context = {}

        if self.kube_config.worker_run_as_user:
            security_context['runAsUser'] = self.kube_config.worker_run_as_user

        if self.kube_config.worker_fs_group:
            security_context['fsGroup'] = self.kube_config.worker_fs_group

        # set fs_group to 65533 if not explicitly specified and using git ssh keypair auth
        if self.kube_config.git_ssh_key_secret_name and security_context.get('fsGroup') is None:
            security_context['fsGroup'] = 65533

        return security_context

    def _get_labels(self, labels):
        copy = self.kube_config.kube_labels.copy()
        copy.update(labels)
        return copy

    def _get_volumes_and_mounts(self):
        def _construct_volume(name, claim, host):
            volume = {
                'name': name
            }
            if claim:
                volume['persistentVolumeClaim'] = {
                    'claimName': claim
                }
            elif host:
                volume['hostPath'] = {
                    'path': host,
                    'type': ''
                }
            else:
                volume['emptyDir'] = {}
            return volume

        volumes = {
            self.dags_volume_name: _construct_volume(
                self.dags_volume_name,
                self.kube_config.dags_volume_claim,
                self.kube_config.dags_volume_host
            ),
            self.logs_volume_name: _construct_volume(
                self.logs_volume_name,
                self.kube_config.logs_volume_claim,
                self.kube_config.logs_volume_host
            )
        }

        volume_mounts = {
            self.dags_volume_name: {
                'name': self.dags_volume_name,
                'mountPath': self.generate_dag_volume_mount_path(),
                'readOnly': True,
            },
            self.logs_volume_name: {
                'name': self.logs_volume_name,
                'mountPath': self.worker_airflow_logs,
            }
        }

        if self.kube_config.dags_volume_subpath:
            volume_mounts[self.dags_volume_name]['subPath'] = self.kube_config.dags_volume_subpath

        if self.kube_config.logs_volume_subpath:
            volume_mounts[self.logs_volume_name]['subPath'] = self.kube_config.logs_volume_subpath

        if self.kube_config.dags_in_image:
            del volumes[self.dags_volume_name]
            del volume_mounts[self.dags_volume_name]

        # Get the SSH key from secrets as a volume
        if self.kube_config.git_ssh_key_secret_name:
            volumes[self.git_sync_ssh_secret_volume_name] = {
                'name': self.git_sync_ssh_secret_volume_name,
                'secret': {
                    'secretName': self.kube_config.git_ssh_key_secret_name,
                    'items': [{
                        'key': self.git_ssh_key_secret_key,
                        'path': 'ssh',
                        'mode': 0o440
                    }]
                }
            }

        if self.kube_config.git_ssh_known_hosts_configmap_name:
            volumes[self.git_sync_ssh_known_hosts_volume_name] = {
                'name': self.git_sync_ssh_known_hosts_volume_name,
                'configMap': {
                    'name': self.kube_config.git_ssh_known_hosts_configmap_name
                },
                'mode': 0o440
            }

        # Mount the airflow.cfg file via a configmap the user has specified
        if self.kube_config.airflow_configmap:
            config_volume_name = 'airflow-config'
            config_path = '{}/airflow.cfg'.format(self.worker_airflow_home)
            volumes[config_volume_name] = {
                'name': config_volume_name,
                'configMap': {
                    'name': self.kube_config.airflow_configmap
                }
            }
            volume_mounts[config_volume_name] = {
                'name': config_volume_name,
                'mountPath': config_path,
                'subPath': 'airflow.cfg',
                'readOnly': True
            }

        return volumes, volume_mounts

    def generate_dag_volume_mount_path(self):
        if self.kube_config.dags_volume_claim or self.kube_config.dags_volume_host:
            dag_volume_mount_path = self.worker_airflow_dags
        else:
            dag_volume_mount_path = self.kube_config.git_dags_folder_mount_point

        return dag_volume_mount_path

    def make_pod(self, namespace, worker_uuid, pod_id, dag_id, task_id, execution_date,
                 try_number, airflow_command, kube_executor_config):
        volumes_dict, volume_mounts_dict = self._get_volumes_and_mounts()
        worker_init_container_spec = self._get_init_containers()
        resources = Resources(
            request_memory=kube_executor_config.request_memory,
            request_cpu=kube_executor_config.request_cpu,
            limit_memory=kube_executor_config.limit_memory,
            limit_cpu=kube_executor_config.limit_cpu
        )
        gcp_sa_key = kube_executor_config.gcp_service_account_key
        annotations = dict(kube_executor_config.annotations) or self.kube_config.kube_annotations
        if gcp_sa_key:
            annotations['iam.cloud.google.com/service-account'] = gcp_sa_key

        volumes = [value for value in volumes_dict.values()] + kube_executor_config.volumes
        volume_mounts = [value for value in volume_mounts_dict.values()] + kube_executor_config.volume_mounts

        affinity = kube_executor_config.affinity or self.kube_config.kube_affinity
        tolerations = kube_executor_config.tolerations or self.kube_config.kube_tolerations

        return Pod(
            namespace=namespace,
            name=pod_id,
            image=kube_executor_config.image or self.kube_config.kube_image,
            image_pull_policy=(kube_executor_config.image_pull_policy or
                               self.kube_config.kube_image_pull_policy),
            cmds=airflow_command,
            labels=self._get_labels({
                'airflow-worker': worker_uuid,
                'dag_id': dag_id,
                'task_id': task_id,
                'execution_date': execution_date,
                'try_number': str(try_number),
            }),
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
            affinity=affinity,
            tolerations=tolerations,
            security_context=self._get_security_context(),
            configmaps=self._get_configmaps()
        )
