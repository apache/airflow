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
from __future__ import annotations

import uuid
from unittest import mock

import pytest
from kubernetes.client import ApiClient, models as k8s

from airflow.exceptions import AirflowConfigException
from airflow.providers.cncf.kubernetes.k8s_model import append_to_pod
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
from airflow.providers.cncf.kubernetes.secret import Secret


class TestSecret:
    def test_to_env_secret(self):
        secret = Secret("env", "name", "secret", "key")
        assert secret.to_env_secret() == k8s.V1EnvVar(
            name="NAME",
            value_from=k8s.V1EnvVarSource(secret_key_ref=k8s.V1SecretKeySelector(name="secret", key="key")),
        )

    def test_to_env_from_secret(self):
        secret = Secret("env", None, "secret")
        assert secret.to_env_from_secret() == k8s.V1EnvFromSource(
            secret_ref=k8s.V1SecretEnvSource(name="secret")
        )

    @mock.patch("uuid.uuid4")
    def test_to_volume_secret(self, mock_uuid):
        mock_uuid.return_value = "0"
        secret = Secret("volume", "/etc/foo", "secret_b")
        assert secret.to_volume_secret() == (
            k8s.V1Volume(name="secretvol0", secret=k8s.V1SecretVolumeSource(secret_name="secret_b")),
            k8s.V1VolumeMount(mount_path="/etc/foo", name="secretvol0", read_only=True),
        )

    @mock.patch("uuid.uuid4")
    def test_only_mount_sub_secret(self, mock_uuid):
        mock_uuid.return_value = "0"
        items = [k8s.V1KeyToPath(key="my-username", path="/extra/path")]
        secret = Secret("volume", "/etc/foo", "secret_b", items=items)
        assert secret.to_volume_secret() == (
            k8s.V1Volume(
                name="secretvol0", secret=k8s.V1SecretVolumeSource(secret_name="secret_b", items=items)
            ),
            k8s.V1VolumeMount(mount_path="/etc/foo", name="secretvol0", read_only=True),
        )

    @mock.patch("uuid.uuid4")
    def test_attach_to_pod(self, mock_uuid, data_file):
        static_uuid = uuid.UUID("cf4a56d2-8101-4217-b027-2af6216feb48")
        mock_uuid.return_value = static_uuid
        template_file = data_file("pods/generator_base.yaml").as_posix()
        pod = PodGenerator(pod_template_file=template_file).ud_pod
        secrets = [
            # This should be a secretRef
            Secret("env", None, "secret_a"),
            # This should be a single secret mounted in volumeMounts
            Secret("volume", "/etc/foo", "secret_b"),
            # This should produce a single secret mounted in env
            Secret("env", "TARGET", "secret_b", "source_b"),
        ]
        k8s_client = ApiClient()
        pod = append_to_pod(pod, secrets)
        result = k8s_client.sanitize_for_serialization(pod)
        assert result == {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "labels": {"app": "myapp"},
                "name": "myapp-pod",
                "namespace": "default",
            },
            "spec": {
                "containers": [
                    {
                        "command": ["sh", "-c", "echo Hello Kubernetes!"],
                        "env": [
                            {"name": "ENVIRONMENT", "value": "prod"},
                            {"name": "LOG_LEVEL", "value": "warning"},
                            {
                                "name": "TARGET",
                                "valueFrom": {"secretKeyRef": {"key": "source_b", "name": "secret_b"}},
                            },
                        ],
                        "envFrom": [
                            {"configMapRef": {"name": "configmap_a"}},
                            {"secretRef": {"name": "secret_a"}},
                        ],
                        "image": "busybox",
                        "name": "base",
                        "ports": [{"containerPort": 1234, "name": "foo"}],
                        "resources": {"limits": {"memory": "200Mi"}, "requests": {"memory": "100Mi"}},
                        "volumeMounts": [
                            {
                                "mountPath": "/etc/foo",
                                "name": f"secretvol{static_uuid}",
                                "readOnly": True,
                            },
                        ],
                    },
                ],
                "hostNetwork": True,
                "imagePullSecrets": [{"name": "pull_secret_a"}, {"name": "pull_secret_b"}],
                "securityContext": {"fsGroup": 2000, "runAsUser": 1000},
                "volumes": [
                    {"name": f"secretvol{static_uuid}", "secret": {"secretName": "secret_b"}},
                ],
            },
        }


class TestSecretConstructor:
    def test_invalid_deploy_type_is_rejected(self):
        with pytest.raises(AirflowConfigException, match="deploy_type must be env or volume"):
            Secret("configmap", "TARGET", "secret_a")

    def test_key_without_deploy_target_is_rejected(self):
        with pytest.raises(AirflowConfigException, match="`deploy_target` should not be None"):
            Secret("env", None, "secret_a", key="username")

    def test_env_deploy_target_is_uppercased(self):
        assert Secret("env", "db_password", "secret_a", "key").deploy_target == "DB_PASSWORD"

    def test_volume_deploy_target_keeps_case(self):
        assert Secret("volume", "/etc/Foo", "secret_a").deploy_target == "/etc/Foo"


class TestSecretAttachToPod:
    @pytest.fixture
    def pod(self) -> k8s.V1Pod:
        return k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")]))

    def test_env_secret_without_key_attaches_env_from(self, pod):
        result = Secret("env", None, "secret_a").attach_to_pod(pod)

        assert result.spec.containers[0].env_from == [
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="secret_a"))
        ]
        assert result.spec.containers[0].env is None

    def test_env_secret_with_key_attaches_env_var(self, pod):
        result = Secret("env", "TARGET", "secret_a", "source").attach_to_pod(pod)

        assert result.spec.containers[0].env == [
            k8s.V1EnvVar(
                name="TARGET",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(name="secret_a", key="source")
                ),
            )
        ]
        assert result.spec.containers[0].env_from is None

    def test_attach_to_pod_does_not_mutate_original_pod(self, pod):
        Secret("volume", "/etc/foo", "secret_a").attach_to_pod(pod)

        assert pod.spec.volumes is None
        assert pod.spec.containers[0].volume_mounts is None


class TestSecretEquality:
    def test_equal_secrets(self):
        assert Secret("env", "TARGET", "secret_a", "key") == Secret("env", "TARGET", "secret_a", "key")
        assert hash(Secret("env", "TARGET", "secret_a", "key")) == hash(
            Secret("env", "TARGET", "secret_a", "key")
        )

    @pytest.mark.parametrize(
        "other",
        [
            Secret("volume", "/etc/foo", "secret_a"),
            Secret("env", "OTHER", "secret_a", "key"),
            Secret("env", "TARGET", "secret_b", "key"),
            Secret("env", "TARGET", "secret_a", "other-key"),
        ],
    )
    def test_different_secrets(self, other):
        assert Secret("env", "TARGET", "secret_a", "key") != other

    def test_repr(self):
        assert repr(Secret("env", "TARGET", "secret_a", "key")) == "Secret(env, TARGET, secret_a, key)"
