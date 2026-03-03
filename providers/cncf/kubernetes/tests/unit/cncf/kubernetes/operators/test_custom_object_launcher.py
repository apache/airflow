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

from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client import (
    CustomObjectsApi,
    V1ContainerState,
    V1ContainerStateWaiting,
    V1ContainerStatus,
    V1Pod,
    V1PodStatus,
)

from airflow.providers.cncf.kubernetes.operators.custom_object_launcher import (
    CustomObjectLauncher,
    SparkJobSpec,
    SparkResources,
)
from airflow.providers.common.compat.sdk import AirflowException


@pytest.fixture
def mock_launcher():
    name = "test-spark-job"
    spec = {
        "image": "gcr.io/spark-operator/spark-py:v3.0.0",
        "driver": {},
        "executor": {},
    }

    custom_object_api = CustomObjectsApi()
    custom_object_api.create_namespaced_custom_object = MagicMock(
        return_value={"spec": spec, "metadata": {"name": name}}
    )

    launcher = CustomObjectLauncher(
        name=name,
        namespace="default",
        kube_client=MagicMock(),
        custom_obj_api=custom_object_api,
        template_body={
            "spark": {
                "spec": spec,
                "apiVersion": "sparkoperator.k8s.io/v1beta2",
                "kind": "SparkApplication",
            },
        },
    )
    launcher.pod_spec = V1Pod()
    launcher.spark_job_not_running = MagicMock(return_value=False)
    return launcher


class TestSparkJobSpec:
    @patch("airflow.providers.cncf.kubernetes.operators.custom_object_launcher.SparkJobSpec.update_resources")
    @patch("airflow.providers.cncf.kubernetes.operators.custom_object_launcher.SparkJobSpec.validate")
    def test_spark_job_spec_initialization(self, mock_validate, mock_update_resources):
        entries = {
            "spec": {
                "dynamicAllocation": {
                    "enabled": True,
                    "initialExecutors": 1,
                    "minExecutors": 1,
                    "maxExecutors": 2,
                },
                "driver": {},
                "executor": {},
            }
        }
        SparkJobSpec(**entries)
        mock_validate.assert_called_once()
        mock_update_resources.assert_called_once()

    def test_spark_job_spec_dynamicAllocation_enabled(self):
        entries = {
            "spec": {
                "dynamicAllocation": {
                    "enabled": True,
                    "initialExecutors": 1,
                    "minExecutors": 1,
                    "maxExecutors": 2,
                },
                "driver": {},
                "executor": {},
            }
        }
        spark_job_spec = SparkJobSpec(**entries)

        assert spark_job_spec.spec["dynamicAllocation"]["enabled"]

    def test_spark_job_spec_dynamicAllocation_enabled_with_default_initial_executors(self):
        entries = {
            "spec": {
                "dynamicAllocation": {
                    "enabled": True,
                    "minExecutors": 1,
                    "maxExecutors": 2,
                },
                "driver": {},
                "executor": {},
            }
        }
        spark_job_spec = SparkJobSpec(**entries)

        assert spark_job_spec.spec["dynamicAllocation"]["enabled"]

    def test_spark_job_spec_dynamicAllocation_enabled_with_invalid_config(self):
        entries = {
            "spec": {
                "dynamicAllocation": {
                    "enabled": True,
                    "initialExecutors": 1,
                    "minExecutors": 1,
                    "maxExecutors": 2,
                },
                "driver": {},
                "executor": {},
            }
        }

        cloned_entries = entries.copy()
        cloned_entries["spec"]["dynamicAllocation"]["minExecutors"] = None
        with pytest.raises(
            AirflowException,
            match="Make sure min/max value for dynamic allocation is passed",
        ):
            SparkJobSpec(**cloned_entries)

        cloned_entries = entries.copy()
        cloned_entries["spec"]["dynamicAllocation"]["maxExecutors"] = None
        with pytest.raises(
            AirflowException,
            match="Make sure min/max value for dynamic allocation is passed",
        ):
            SparkJobSpec(**cloned_entries)


class TestSparkResources:
    @patch(
        "airflow.providers.cncf.kubernetes.operators.custom_object_launcher.SparkResources.convert_resources"
    )
    def test_spark_resources_initialization(self, mock_convert_resources):
        driver = {
            "gpu": {"name": "nvidia", "quantity": 1},
            "cpu": {"request": "1", "limit": "2"},
            "memory": {"request": "1Gi", "limit": "2Gi"},
        }
        executor = {
            "gpu": {"name": "nvidia", "quantity": 2},
            "cpu": {"request": "2", "limit": "4"},
            "memory": {"request": "2Gi", "limit": "4Gi"},
        }
        SparkResources(driver=driver, executor=executor)
        mock_convert_resources.assert_called_once()

    def test_spark_resources_conversion(self):
        driver = {
            "gpu": {"name": "nvidia", "quantity": 1},
            "cpu": {"request": "1", "limit": "2"},
            "memory": {"request": "1Gi", "limit": "2Gi"},
        }
        executor = {
            "gpu": {"name": "nvidia", "quantity": 2},
            "cpu": {"request": "2", "limit": "4"},
            "memory": {"request": "2Gi", "limit": "4Gi"},
        }
        spark_resources = SparkResources(driver=driver, executor=executor)

        assert spark_resources.driver["memory"]["limit"] == "1462m"
        assert spark_resources.executor["memory"]["limit"] == "2925m"
        assert spark_resources.driver["cpu"]["request"] == 1
        assert spark_resources.driver["cpu"]["limit"] == "2"
        assert spark_resources.executor["cpu"]["request"] == 2
        assert spark_resources.executor["cpu"]["limit"] == "4"
        assert spark_resources.driver["gpu"]["quantity"] == 1
        assert spark_resources.executor["gpu"]["quantity"] == 2


class TestCustomObjectLauncher:
    def get_pod_status(self, reason: str, message: str | None = None):
        return V1PodStatus(
            container_statuses=[
                V1ContainerStatus(
                    image="test",
                    image_id="test",
                    name="test",
                    ready=False,
                    restart_count=0,
                    state=V1ContainerState(
                        waiting=V1ContainerStateWaiting(
                            reason=reason,
                            message=message,
                        ),
                    ),
                ),
            ]
        )

    def test_get_body_initializes_metadata_when_missing(self, mock_launcher):
        mock_launcher.template_body["spark"].pop("metadata", None)
        body = mock_launcher.get_body()
        assert isinstance(body["metadata"], dict)
        assert body["metadata"]["name"] == mock_launcher.name
        assert body["metadata"]["namespace"] == mock_launcher.namespace

    def test_get_body_replaces_non_dict_metadata(self, mock_launcher):
        mock_launcher.template_body["spark"]["metadata"] = "not-a-dict"
        body = mock_launcher.get_body()
        assert isinstance(body["metadata"], dict)
        assert body["metadata"]["name"] == mock_launcher.name
        assert body["metadata"]["namespace"] == mock_launcher.namespace

    def test_get_body_preserves_existing_metadata_labels(self, mock_launcher):
        mock_launcher.template_body["spark"]["metadata"] = {"labels": {"team": "data"}}
        body = mock_launcher.get_body()
        assert body["metadata"]["labels"]["team"] == "data"
        assert body["metadata"]["name"] == mock_launcher.name
        assert body["metadata"]["namespace"] == mock_launcher.namespace

    @patch("airflow.providers.cncf.kubernetes.operators.custom_object_launcher.PodManager")
    def test_start_spark_job_no_error(self, mock_pod_manager, mock_launcher):
        mock_launcher.start_spark_job()

    @patch("airflow.providers.cncf.kubernetes.operators.custom_object_launcher.PodManager")
    def test_check_pod_start_failure_no_error(self, mock_pod_manager, mock_launcher):
        mock_pod_manager.return_value.read_pod.return_value.status = self.get_pod_status("ContainerCreating")
        mock_launcher.check_pod_start_failure()

        mock_pod_manager.return_value.read_pod.return_value.status = self.get_pod_status("PodInitializing")
        mock_launcher.check_pod_start_failure()

    @patch("airflow.providers.cncf.kubernetes.operators.custom_object_launcher.PodManager")
    def test_check_pod_start_failure_with_error(self, mock_pod_manager, mock_launcher):
        mock_pod_manager.return_value.read_pod.return_value.status = self.get_pod_status(
            "CrashLoopBackOff", "Error message"
        )
        with pytest.raises(AirflowException):
            mock_launcher.check_pod_start_failure()
