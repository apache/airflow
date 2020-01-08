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
"""Configuration of the worker"""
import os
from typing import Dict, List

import kubernetes.client.models as k8s

from airflow.configuration import conf
from airflow.kubernetes.k8s_model import append_to_pod
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.kubernetes.secret import Secret
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.version import version as airflow_version


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

    def _get_init_containers(self) -> List[k8s.V1Container]:
        """When using git to retrieve the DAGs, use the GitSync Init Container"""
        # If we're using volume claims to mount the dags, no init container is needed
        if self.kube_config.dags_volume_claim or \
           self.kube_config.dags_volume_host or self.kube_config.dags_in_image:
            return []

        # Otherwise, define a git-sync init container
        init_environment = [k8s.V1EnvVar(
            name='GIT_SYNC_REPO',
            value=self.kube_config.git_repo
        ), k8s.V1EnvVar(
            name='GIT_SYNC_BRANCH',
            value=self.kube_config.git_branch
        ), k8s.V1EnvVar(
            name='GIT_SYNC_ROOT',
            value=self.kube_config.git_sync_root
        ), k8s.V1EnvVar(
            name='GIT_SYNC_DEST',
            value=self.kube_config.git_sync_dest
        ), k8s.V1EnvVar(
            name='GIT_SYNC_REV',
            value=self.kube_config.git_sync_rev
        ), k8s.V1EnvVar(
            name='GIT_SYNC_DEPTH',
            value='1'
        ), k8s.V1EnvVar(
            name='GIT_SYNC_ONE_TIME',
            value='true'
        )]
        if self.kube_config.git_user:
            init_environment.append(k8s.V1EnvVar(
                name='GIT_SYNC_USERNAME',
                value=self.kube_config.git_user
            ))
        if self.kube_config.git_password:
            init_environment.append(k8s.V1EnvVar(
                name='GIT_SYNC_PASSWORD',
                value=self.kube_config.git_password
            ))

        volume_mounts = [k8s.V1VolumeMount(
            mount_path=self.kube_config.git_sync_root,
            name=self.dags_volume_name,
            read_only=False
        )]

        if self.kube_config.git_sync_credentials_secret:
            init_environment.extend([
                k8s.V1EnvVar(
                    name='GIT_SYNC_USERNAME',
                    value_from=k8s.V1EnvVarSource(
                        secret_key_ref=k8s.V1SecretKeySelector(
                            name=self.kube_config.git_sync_credentials_secret,
                            key='GIT_SYNC_USERNAME')
                    )
                ),
                k8s.V1EnvVar(
                    name='GIT_SYNC_PASSWORD',
                    value_from=k8s.V1EnvVarSource(
                        secret_key_ref=k8s.V1SecretKeySelector(
                            name=self.kube_config.git_sync_credentials_secret,
                            key='GIT_SYNC_PASSWORD')
                    )
                )
            ])

        if self.kube_config.git_ssh_key_secret_name:
            volume_mounts.append(k8s.V1VolumeMount(
                name=self.git_sync_ssh_secret_volume_name,
                mount_path='/etc/git-secret/ssh',
                sub_path='ssh'
            ))

            init_environment.extend([
                k8s.V1EnvVar(
                    name='GIT_SSH_KEY_FILE',
                    value='/etc/git-secret/ssh'
                ),
                k8s.V1EnvVar(
                    name='GIT_SYNC_SSH',
                    value='true'
                )
            ])

        if self.kube_config.git_ssh_known_hosts_configmap_name:
            volume_mounts.append(k8s.V1VolumeMount(
                name=self.git_sync_ssh_known_hosts_volume_name,
                mount_path='/etc/git-secret/known_hosts',
                sub_path='known_hosts'
            ))
            init_environment.extend([k8s.V1EnvVar(
                name='GIT_KNOWN_HOSTS',
                value='true'
            ), k8s.V1EnvVar(
                name='GIT_SSH_KNOWN_HOSTS_FILE',
                value='/etc/git-secret/known_hosts'
            )])
        else:
            init_environment.append(k8s.V1EnvVar(
                name='GIT_KNOWN_HOSTS',
                value='false'
            ))

        init_containers = k8s.V1Container(
            name=self.kube_config.git_sync_init_container_name,
            image=self.kube_config.git_sync_container,
            env=init_environment,
            volume_mounts=volume_mounts
        )

        if self.kube_config.git_sync_run_as_user != "":
            init_containers.security_context = k8s.V1SecurityContext(
                run_as_user=self.kube_config.git_sync_run_as_user
            )  # git-sync user

        return [init_containers]

    def _get_environment(self) -> Dict[str, str]:
        """Defines any necessary environment variables for the pod executor"""
        env = {}

        for env_var_name, env_var_val in self.kube_config.kube_env_vars.items():
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

    def _get_env_from(self) -> List[k8s.V1EnvFromSource]:
        """Extracts any configmapRefs to envFrom"""
        env_from = []

        if self.kube_config.env_from_configmap_ref:
            for config_map_ref in self.kube_config.env_from_configmap_ref.split(','):
                env_from.append(
                    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(config_map_ref))
                )

        if self.kube_config.env_from_secret_ref:
            for secret_ref in self.kube_config.env_from_secret_ref.split(','):
                env_from.append(
                    k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(secret_ref))
                )

        return env_from

    def _get_secrets(self):
        """Defines any necessary secrets for the pod executor"""
        worker_secrets = []

        for env_var_name, obj_key_pair in self.kube_config.kube_secrets.items():
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

    def _get_security_context(self) -> k8s.V1PodSecurityContext:
        """Defines the security context"""

        security_context = k8s.V1PodSecurityContext()

        if self.kube_config.worker_run_as_user != "":
            security_context.run_as_user = self.kube_config.worker_run_as_user

        if self.kube_config.worker_fs_group != "":
            security_context.fs_group = self.kube_config.worker_fs_group

        # set fs_group to 65533 if not explicitly specified and using git ssh keypair auth
        if self.kube_config.git_ssh_key_secret_name and security_context.fs_group is None:
            security_context.fs_group = 65533

        return security_context

    def _get_labels(self, kube_executor_labels, labels) -> k8s.V1LabelSelector:
        copy = self.kube_config.kube_labels.copy()
        copy.update(kube_executor_labels)
        copy.update(labels)
        return copy

    def _get_volume_mounts(self) -> List[k8s.V1VolumeMount]:
        volume_mounts = {
            self.dags_volume_name: k8s.V1VolumeMount(
                name=self.dags_volume_name,
                mount_path=self.generate_dag_volume_mount_path(),
                read_only=True,
            ),
            self.logs_volume_name: k8s.V1VolumeMount(
                name=self.logs_volume_name,
                mount_path=self.worker_airflow_logs,
            )
        }

        if self.kube_config.dags_volume_subpath:
            volume_mounts[self.dags_volume_name].sub_path = self.kube_config.dags_volume_subpath

        if self.kube_config.logs_volume_subpath:
            volume_mounts[self.logs_volume_name].sub_path = self.kube_config.logs_volume_subpath

        if self.kube_config.dags_in_image:
            del volume_mounts[self.dags_volume_name]

        # Mount the airflow.cfg file via a configmap the user has specified
        if self.kube_config.airflow_configmap:
            config_volume_name = 'airflow-config'
            config_path = '{}/airflow.cfg'.format(self.worker_airflow_home)
            volume_mounts[config_volume_name] = k8s.V1VolumeMount(
                name=config_volume_name,
                mount_path=config_path,
                sub_path='airflow.cfg',
                read_only=True
            )

        # Mount the airflow_local_settings.py file via a configmap the user has specified
        if self.kube_config.airflow_local_settings_configmap:
            config_volume_name = 'airflow-config'
            config_path = '{}/config/airflow_local_settings.py'.format(self.worker_airflow_home)
            volume_mounts[config_volume_name] = k8s.V1VolumeMount(
                name=config_volume_name,
                mount_path=config_path,
                sub_path='airflow_local_settings.py',
                read_only=True
            )

        return list(volume_mounts.values())

    def _get_volumes(self) -> List[k8s.V1Volume]:
        def _construct_volume(name, claim, host) -> k8s.V1Volume:
            volume = k8s.V1Volume(name=name)

            if claim:
                volume.persistent_volume_claim = k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name=claim
                )
            elif host:
                volume.host_path = k8s.V1HostPathVolumeSource(
                    path=host,
                    type=''
                )
            else:
                volume.empty_dir = {}

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

        if self.kube_config.dags_in_image:
            del volumes[self.dags_volume_name]

        # Get the SSH key from secrets as a volume
        if self.kube_config.git_ssh_key_secret_name:
            volumes[self.git_sync_ssh_secret_volume_name] = k8s.V1Volume(
                name=self.git_sync_ssh_secret_volume_name,
                secret=k8s.V1SecretVolumeSource(
                    secret_name=self.kube_config.git_ssh_key_secret_name,
                    items=[k8s.V1KeyToPath(
                        key=self.git_ssh_key_secret_key,
                        path='ssh',
                        mode=0o440
                    )]
                )
            )

        if self.kube_config.git_ssh_known_hosts_configmap_name:
            volumes[self.git_sync_ssh_known_hosts_volume_name] = k8s.V1Volume(
                name=self.git_sync_ssh_known_hosts_volume_name,
                config_map=k8s.V1ConfigMapVolumeSource(
                    name=self.kube_config.git_ssh_known_hosts_configmap_name,
                    default_mode=0o440
                )
            )

        # Mount the airflow.cfg file via a configmap the user has specified
        if self.kube_config.airflow_configmap:
            config_volume_name = 'airflow-config'
            volumes[config_volume_name] = k8s.V1Volume(
                name=config_volume_name,
                config_map=k8s.V1ConfigMapVolumeSource(
                    name=self.kube_config.airflow_configmap
                )
            )

        # Mount the airflow_local_settings.py file via a configmap the user has specified
        if self.kube_config.airflow_local_settings_configmap:
            config_volume_name = 'airflow-config'
            volumes[config_volume_name] = k8s.V1Volume(
                name=config_volume_name,
                config_map=k8s.V1ConfigMapVolumeSource(
                    name=self.kube_config.airflow_local_settings_configmap
                )
            )

        return list(volumes.values())

    def generate_dag_volume_mount_path(self) -> str:
        """Generate path for DAG volume"""
        if self.kube_config.dags_volume_claim or self.kube_config.dags_volume_host:
            return self.worker_airflow_dags

        return self.kube_config.git_dags_folder_mount_point

    def make_pod(self, namespace, worker_uuid, pod_id, dag_id, task_id, execution_date,
                 try_number, airflow_command) -> k8s.V1Pod:
        """Creates POD."""
        pod_generator = PodGenerator(
            namespace=namespace,
            name=pod_id,
            image=self.kube_config.kube_image,
            image_pull_policy=self.kube_config.kube_image_pull_policy,
            image_pull_secrets=self.kube_config.image_pull_secrets,
            labels={
                'airflow-worker': worker_uuid,
                'dag_id': dag_id,
                'task_id': task_id,
                'execution_date': execution_date,
                'try_number': str(try_number),
                'airflow_version': airflow_version.replace('+', '-'),
                'kubernetes_executor': 'True',
            },
            cmds=airflow_command,
            volumes=self._get_volumes(),
            volume_mounts=self._get_volume_mounts(),
            init_containers=self._get_init_containers(),
            annotations=self.kube_config.kube_annotations,
            affinity=self.kube_config.kube_affinity,
            tolerations=self.kube_config.kube_tolerations,
            envs=self._get_environment(),
            node_selectors=self.kube_config.kube_node_selectors,
            service_account_name=self.kube_config.worker_service_account_name,
        )

        pod = pod_generator.gen_pod()
        pod.spec.containers[0].env_from = pod.spec.containers[0].env_from or []
        pod.spec.containers[0].env_from.extend(self._get_env_from())
        pod.spec.security_context = self._get_security_context()

        return append_to_pod(pod, self._get_secrets())
