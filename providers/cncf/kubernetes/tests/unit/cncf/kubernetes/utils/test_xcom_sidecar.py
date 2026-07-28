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

import pytest
from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults, add_xcom_sidecar


def _base_pod() -> k8s.V1Pod:
    return k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")]))


def _sidecar_of(pod: k8s.V1Pod) -> k8s.V1Container:
    return next(c for c in pod.spec.containers if c.name == PodDefaults.SIDECAR_CONTAINER_NAME)


@pytest.mark.parametrize(
    "security_context",
    [
        pytest.param(
            {
                "allowPrivilegeEscalation": False,
                "readOnlyRootFilesystem": True,
                "seccompProfile": {"type": "RuntimeDefault"},
            },
            id="dict",
        ),
        pytest.param(
            k8s.V1SecurityContext(
                allow_privilege_escalation=False,
                read_only_root_filesystem=True,
                seccomp_profile=k8s.V1SeccompProfile(type="RuntimeDefault"),
            ),
            id="model",
        ),
    ],
)
def test_add_xcom_sidecar_sets_security_context(security_context):
    pod = add_xcom_sidecar(_base_pod(), sidecar_container_security_context=security_context)
    assert _sidecar_of(pod).security_context == security_context


def test_add_xcom_sidecar_without_security_context():
    pod = add_xcom_sidecar(_base_pod())
    assert _sidecar_of(pod).security_context is None


def test_add_xcom_sidecar_empty_security_context():
    pod = add_xcom_sidecar(_base_pod(), sidecar_container_security_context={})
    assert _sidecar_of(pod).security_context == {}


def test_add_xcom_sidecar_does_not_mutate_shared_default():
    add_xcom_sidecar(_base_pod(), sidecar_container_security_context={"readOnlyRootFilesystem": True})
    assert PodDefaults.SIDECAR_CONTAINER.security_context is None
