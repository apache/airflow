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

from airflow.providers.cncf.kubernetes.k8s_model import K8SModel, append_to_pod


class _LabelAttachingModel(K8SModel):
    """Attach one label so the order of application is observable on the pod."""

    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value

    def attach_to_pod(self, pod: k8s.V1Pod) -> k8s.V1Pod:
        if pod.metadata is None:
            pod.metadata = k8s.V1ObjectMeta(labels={})
        pod.metadata.labels[self.key] = self.value
        return pod


@pytest.fixture
def pod() -> k8s.V1Pod:
    return k8s.V1Pod(spec=k8s.V1PodSpec(containers=[k8s.V1Container(name="base")]))


def test_k8s_model_is_abstract():
    with pytest.raises(TypeError, match="abstract"):
        K8SModel()  # type: ignore[abstract]


def test_append_to_pod_without_objects_returns_pod_unchanged(pod):
    assert append_to_pod(pod, None) is pod
    assert append_to_pod(pod, []) is pod


def test_append_to_pod_applies_each_object(pod):
    result = append_to_pod(pod, [_LabelAttachingModel("a", "1"), _LabelAttachingModel("b", "2")])

    assert result.metadata.labels == {"a": "1", "b": "2"}


def test_append_to_pod_applies_objects_in_order(pod):
    result = append_to_pod(
        pod, [_LabelAttachingModel("winner", "first"), _LabelAttachingModel("winner", "second")]
    )

    assert result.metadata.labels == {"winner": "second"}
