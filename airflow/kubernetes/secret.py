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
"""Classes for interacting with Kubernetes API"""
import copy
import uuid
from typing import Tuple

from kubernetes.client import models as k8s

from airflow.exceptions import AirflowConfigException
from airflow.kubernetes.k8s_model import K8SModel


class Secret(K8SModel):
    """Defines Kubernetes Secret Volume"""

    def __init__(self, deploy_type, deploy_target, secret, key=None, items=None):
        """
        Initialize a Kubernetes Secret Object. Used to track requested secrets from
        the user.

        :param deploy_type: The type of secret deploy in Kubernetes, either `env` or
            `volume`
        :type deploy_type: str
        :param deploy_target: (Optional) The environment variable when
            `deploy_type` `env` or file path when `deploy_type` `volume` where
            expose secret. If `key` is not provided deploy target should be None.
        :type deploy_target: str or None
        :param secret: Name of the secrets object in Kubernetes
        :type secret: str
        :param key: (Optional) Key of the secret within the Kubernetes Secret
            if not provided in `deploy_type` `env` it will mount all secrets in object
        :type key: str or None
        :param items: (Optional) items that can be added to a volume secret for specifying projects of
        secret keys to paths
        https://kubernetes.io/docs/concepts/configuration/secret/#projection-of-secret-keys-to-specific-paths
        :type items: List[k8s.V1KeyToPath]
        """
        if deploy_type not in ('env', 'volume'):
            raise AirflowConfigException("deploy_type must be env or volume")

        self.deploy_type = deploy_type
        self.deploy_target = deploy_target
        self.items = items or []

        if deploy_target is not None and deploy_type == 'env':
            # if deploying to env, capitalize the deploy target
            self.deploy_target = deploy_target.upper()

        if key is not None and deploy_target is None:
            raise AirflowConfigException(
                'If `key` is set, `deploy_target` should not be None'
            )

        self.secret = secret
        self.key = key

    def to_env_secret(self) -> k8s.V1EnvVar:
        """Stores es environment secret"""
        return k8s.V1EnvVar(
            name=self.deploy_target,
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name=self.secret,
                    key=self.key
                )
            )
        )

    def to_env_from_secret(self) -> k8s.V1EnvFromSource:
        """Reads from environment to secret"""
        return k8s.V1EnvFromSource(
            secret_ref=k8s.V1SecretEnvSource(name=self.secret)
        )

    def to_volume_secret(self) -> Tuple[k8s.V1Volume, k8s.V1VolumeMount]:
        """Converts to volume secret"""
        vol_id = 'secretvol{}'.format(uuid.uuid4())
        volume = k8s.V1Volume(name=vol_id, secret=k8s.V1SecretVolumeSource(secret_name=self.secret))
        if self.items:
            volume.secret.items = self.items
        return (
            volume,
            k8s.V1VolumeMount(
                mount_path=self.deploy_target,
                name=vol_id,
                read_only=True
            )
        )

    def attach_to_pod(self, pod: k8s.V1Pod) -> k8s.V1Pod:
        """Attaches to pod"""
        cp_pod = copy.deepcopy(pod)
        if self.deploy_type == 'volume':
            volume, volume_mount = self.to_volume_secret()
            cp_pod.spec.volumes = pod.spec.volumes or []
            cp_pod.spec.volumes.append(volume)
            cp_pod.spec.containers[0].volume_mounts = pod.spec.containers[0].volume_mounts or []
            cp_pod.spec.containers[0].volume_mounts.append(volume_mount)
        if self.deploy_type == 'env' and self.key is not None:
            env = self.to_env_secret()
            cp_pod.spec.containers[0].env = cp_pod.spec.containers[0].env or []
            cp_pod.spec.containers[0].env.append(env)
        if self.deploy_type == 'env' and self.key is None:
            env_from = self.to_env_from_secret()
            cp_pod.spec.containers[0].env_from = cp_pod.spec.containers[0].env_from or []
            cp_pod.spec.containers[0].env_from.append(env_from)
        return cp_pod

    def __eq__(self, other):
        return (
            self.deploy_type == other.deploy_type and
            self.deploy_target == other.deploy_target and
            self.secret == other.secret and
            self.key == other.key
        )

    def __repr__(self):
        return 'Secret({}, {}, {}, {})'.format(
            self.deploy_type,
            self.deploy_target,
            self.secret,
            self.key
        )
