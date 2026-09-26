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

from unittest import mock

import pytest
from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.k8s_model import K8SModel, append_to_pod


@pytest.fixture
def pod() -> k8s.V1Pod:
    return k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="base"))


class TestAppendToPod:
    @pytest.mark.parametrize("k8s_objects", [None, []], ids=["none", "empty"])
    def test_no_objects_returns_pod_unchanged(self, pod, k8s_objects):
        assert append_to_pod(pod, k8s_objects) is pod

    def test_each_object_receives_previous_result(self, pod):
        intermediate = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="intermediate"))
        final = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="final"))
        first = mock.create_autospec(K8SModel, instance=True)
        first.attach_to_pod.return_value = intermediate
        second = mock.create_autospec(K8SModel, instance=True)
        second.attach_to_pod.return_value = final

        assert append_to_pod(pod, [first, second]) is final
        first.attach_to_pod.assert_called_once_with(pod)
        second.attach_to_pod.assert_called_once_with(intermediate)
