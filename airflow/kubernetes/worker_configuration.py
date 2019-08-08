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

from airflow.configuration import conf
import kubernetes.client.models as k8s
from airflow.kubernetes.pod_generator import PodGenerator
from airflow.utils.log.logging_mixin import LoggingMixin
from typing import List


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
        init_environment: List[k8s.V1EnvVar] = [k8s.V1EnvVar(
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

        volume_mounts: List[k8s.V1VolumeMount] = [k8s.V1VolumeMount(
            mount_path=self.kube_config.git_sync_root,
            name=self.dags_volume_name,
            read_only=False
        )]
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

        return [k8s.V1Container(
            name=self.kube_config.git_sync_init_container_name,
            image=self.kube_config.git_sync_container,
            security_context=k8s.V1SecurityContext(run_as_user=self.kube_config.git_sync_run_as_user or 65533),  # git-sync user
            env=init_environment,
            volume_mounts=volume_mounts,
        )]

    def _get_env(self) -> List[k8s.V1EnvVar]:
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

        return list(map(lambda tup: k8s.V1EnvVar(name=tup[0], value=tup[1]), env.items()))

    def _get_env_from(self) -> List[k8s.V1EnvFromSource]:
        """Extracts any configmapRefs to envFrom"""
        env_from = []

        if self.kube_config.env_from_configmap_ref:
            for config_map_ref in self.kube_config.env_from_secret_ref.split(','):
                env_from.append(
                    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(config_map_ref))
                )

        if self.kube_config.env_from_secret_ref:
            for secret_ref in self.kube_config.env_from_secret_ref.split(','):
                env_from.append(
                    k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(secret_ref))
                )

        return env_from

    def _get_secret_env(self) -> List[k8s.V1EnvVar]:
        """Defines any necessary secrets for the pod executor"""
        worker_secrets: List[k8s.V1EnvVar] = []

        for env_var_name, obj_key_pair in self.kube_config.kube_secrets.items():
            k8s_secret_obj, k8s_secret_key = obj_key_pair.split('=')
            worker_secrets.append(
                k8s.V1EnvVar(
                    name=env_var_name,
                    value_from=k8s.V1EnvVarSource(
                        secret_key_ref=k8s.V1SecretKeySelector(
                            name=k8s_secret_obj,
                            key=k8s_secret_key
                        )
                    )
                )
            )

        return worker_secrets

    def _get_image_pull_secrets(self) -> List[k8s.V1LocalObjectReference]:
        """Extracts any image pull secrets for fetching container(s)"""
        if not self.kube_config.image_pull_secrets:
            return []
        pull_secrets = self.kube_config.image_pull_secrets.split(',')
        return list(map(lambda name: k8s.V1LocalObjectReference(name), pull_secrets))

    def _get_security_context(self) -> k8s.V1PodSecurityContext:
        """Defines the security context"""

        fs_group = None

        if self.kube_config.worker_fs_group:
            fs_group = self.kube_config.worker_fs_group

        # set fs_group to 65533 if not explicitly specified and using git ssh keypair auth
        if self.kube_config.git_ssh_key_secret_name and fs_group is None:
            fs_group = 65533

        return k8s.V1PodSecurityContext(
            run_as_user=self.kube_config.worker_run_as_user or None,
            fs_group=fs_group
        )

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

        return list(volumes.values())

    def generate_dag_volume_mount_path(self):
        if self.kube_config.dags_volume_claim or self.kube_config.dags_volume_host:
            dag_volume_mount_path = self.worker_airflow_dags
        else:
            dag_volume_mount_path = self.kube_config.git_dags_folder_mount_point

        return dag_volume_mount_path

    def make_pod(self, namespace, worker_uuid, pod_id, dag_id, task_id, execution_date,
                 try_number, airflow_command):
        pod_generator = PodGenerator(

            namespace=namespace,
            name=pod_id,
            image=self.kube_config.kube_image,
            image_pull_policy=self.kube_config.kube_image_pull_policy,
            labels={
                'airflow-worker': worker_uuid,
                'dag_id': dag_id,
                'task_id': task_id,
                'execution_date': execution_date,
                'try_number': str(try_number),
            },
            cmds=[airflow_command],
            volumes=self._get_volumes(),
            volume_mounts=self._get_volume_mounts(),
            init_containers=self._get_init_containers(),
            annotations=self.kube_config.kube_annotations,
            affinity=self.kube_config.kube_affinity,
            tolerations=self.kube_config.kube_tolerations,
            configmaps=self._get_env_from(),
            security_context=self._get_security_context(),
            envs=self._get_env() + self._get_secret_env(),
            node_selectors=self.kube_config.kube_node_selectors,
            service_account_name=self.kube_config.worker_service_account_name,
        )

        return pod_generator.gen_pod()
