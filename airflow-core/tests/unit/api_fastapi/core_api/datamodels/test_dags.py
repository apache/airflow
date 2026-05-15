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

from datetime import datetime, timedelta, timezone

import pytest

from airflow._shared.module_loading import qualname
from airflow.api_fastapi.core_api.datamodels.dags import DAGDetailsResponse


class TestGetDefaultArgsValidator:
    """Test the get_default_args field_validator on DAGDetailsResponse."""

    def _call_validator(self, value):
        """Invoke the classmethod validator directly."""
        return DAGDetailsResponse.get_default_args(value)

    def test_none_returns_none(self):
        assert self._call_validator(None) is None

    def test_plain_dict_is_preserved(self):
        result = self._call_validator({"retries": 3, "depends_on_past": False})
        assert result == {"retries": 3, "depends_on_past": False}

    def test_timedelta_values_are_preserved(self):
        td = timedelta(minutes=5)
        result = self._call_validator({"retry_delay": td})
        assert result == {"retry_delay": td}

    def test_datetime_values_are_preserved(self):
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = self._call_validator({"start_date": start_date})
        assert result == {"start_date": start_date}

    def test_pod_override_is_replaced_with_type_name(self):
        k8s = pytest.importorskip("kubernetes.client.models")
        pod = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="test-pod"))
        result = self._call_validator({"executor_config": {"pod_override": pod, "namespace": "custom"}})
        assert result == {"executor_config": {"pod_override": qualname(pod), "namespace": "custom"}}

    @pytest.mark.parametrize(
        "pod_override",
        [
            pytest.param(None, id="none"),
            pytest.param("already-serialized", id="string"),
            pytest.param({"metadata": {"name": "pod"}}, id="dict"),
            pytest.param([{"metadata": {"name": "pod"}}], id="list"),
        ],
    )
    def test_serialized_pod_override_values_are_preserved(self, pod_override):
        result = self._call_validator({"executor_config": {"pod_override": pod_override}})
        assert result == {"executor_config": {"pod_override": pod_override}}

    def test_serialized_pod_override_preserves_other_executor_config_keys(self):
        executor_config = {
            "pod_override": {"metadata": {"name": "pod"}},
            "KubernetesExecutor": {"image": "custom-image"},
        }

        result = self._call_validator({"executor_config": executor_config})

        assert result == {"executor_config": executor_config}

    def test_non_serialized_pod_override_object_is_replaced_with_type_name(self):
        class Opaque:
            pass

        value = Opaque()
        result = self._call_validator({"executor_config": {"pod_override": value}})
        assert result == {"executor_config": {"pod_override": qualname(value)}}

    def test_non_pod_override_objects_are_left_unchanged(self):
        class Opaque:
            def to_dict(self):
                return {"password": "secret"}

        value = Opaque()
        result = self._call_validator({"connection": value})
        assert result["connection"] is value
