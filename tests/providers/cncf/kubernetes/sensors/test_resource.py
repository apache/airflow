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

from unittest.mock import patch

import pytest

from airflow import DAG
from airflow.utils import timezone

TEST_CRD_RESOURCE = """
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
"""
HOOK_CLASS = "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook"


@patch("airflow.utils.context.Context")
class TestKubernetesXResourceSensor:
    @pytest.fixture(autouse=True)
    def setup_tests(self, dag_maker):
        self._default_client_patch = patch(f"{HOOK_CLASS}._get_default_client")
        self._default_client_mock = self._default_client_patch.start()

        yield

        patch.stopall()

    def setup_method(self):
        args = {"owner": "airflow", "start_date": timezone.datetime(2020, 2, 1)}
        self.dag = DAG("test_dag_id", default_args=args)

    # @patch(
    #     "kubernetes.dynamic.client.DynamicClient.resources.get",
    #     return_value={"items": [{"status": {"jobStatus": "RUNNING"}}]},
    # )
    # def test_pending_rerun_application(self, mock_get_namespaced_crd, context):
    #     op = JobKubernetesResourceSensor(
    #         yaml_conf=TEST_CRD_RESOURCE,
    #         dag=self.dag,
    #         task_id="test_task_id")
    #
    #     op.execute(context)
    #
    #     mock_get_namespaced_crd.assert_called_once_with(
    #         group="ray.io",
    #         name="rayjob-sample",
    #         namespace="default",
    #         plural="rayjobs",
    #         version="v1",
    #     )
