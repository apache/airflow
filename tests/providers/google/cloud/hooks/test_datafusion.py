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

import json
from unittest import mock

import pytest

from airflow.providers.google.cloud.hooks.datafusion import SUCCESS_STATES, DataFusionHook, PipelineStates
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

API_VERSION = "v1beta1"
GCP_CONN_ID = "google_cloud_default"
HOOK_STR = "airflow.providers.google.cloud.hooks.datafusion.{}"
LOCATION = "test-location"
INSTANCE_NAME = "airflow-test-instance"
INSTANCE = {"type": "BASIC", "displayName": INSTANCE_NAME}
PROJECT_ID = "test_project_id"
PIPELINE_NAME = "shrubberyPipeline"
PIPELINE = {"test": "pipeline"}
INSTANCE_URL = "http://datafusion.instance.com"
RUNTIME_ARGS = {"arg1": "a", "arg2": "b"}

# pylint: disable=redefined-outer-name


@pytest.fixture
def hook():
    with mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    ):
        yield DataFusionHook(gcp_conn_id=GCP_CONN_ID)


class TestDataFusionHook:
    @staticmethod
    def mock_endpoint(get_conn_mock):
        return get_conn_mock.return_value.projects.return_value.locations.return_value.instances.return_value

    def test_name(self, hook):
        expected = f"projects/{PROJECT_ID}/locations/{LOCATION}/instances/{INSTANCE_NAME}"
        assert hook._name(PROJECT_ID, LOCATION, INSTANCE_NAME) == expected

    def test_parent(self, hook):
        expected = f"projects/{PROJECT_ID}/locations/{LOCATION}"
        assert hook._parent(PROJECT_ID, LOCATION) == expected

    @mock.patch(HOOK_STR.format("build"))
    @mock.patch(HOOK_STR.format("DataFusionHook._authorize"))
    def test_get_conn(self, mock_authorize, mock_build, hook):
        mock_authorize.return_value = "test"
        hook.get_conn()
        mock_build.assert_called_once_with("datafusion", hook.api_version, http="test", cache_discovery=False)

    @mock.patch(HOOK_STR.format("DataFusionHook.get_conn"))
    def test_restart_instance(self, get_conn_mock, hook):
        method_mock = self.mock_endpoint(get_conn_mock).restart
        method_mock.return_value.execute.return_value = "value"
        result = hook.restart_instance(instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID)

        assert result == "value"
        method_mock.assert_called_once_with(name=hook._name(PROJECT_ID, LOCATION, INSTANCE_NAME))

    @mock.patch(HOOK_STR.format("DataFusionHook.get_conn"))
    def test_delete_instance(self, get_conn_mock, hook):
        method_mock = self.mock_endpoint(get_conn_mock).delete
        method_mock.return_value.execute.return_value = "value"
        result = hook.delete_instance(instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID)

        assert result == "value"
        method_mock.assert_called_once_with(name=hook._name(PROJECT_ID, LOCATION, INSTANCE_NAME))

    @mock.patch(HOOK_STR.format("DataFusionHook.get_conn"))
    def test_create_instance(self, get_conn_mock, hook):
        method_mock = self.mock_endpoint(get_conn_mock).create
        method_mock.return_value.execute.return_value = "value"
        result = hook.create_instance(
            instance_name=INSTANCE_NAME,
            instance=INSTANCE,
            location=LOCATION,
            project_id=PROJECT_ID,
        )

        assert result == "value"
        method_mock.assert_called_once_with(
            parent=hook._parent(PROJECT_ID, LOCATION),
            body=INSTANCE,
            instanceId=INSTANCE_NAME,
        )

    @mock.patch(HOOK_STR.format("DataFusionHook.get_conn"))
    def test_patch_instance(self, get_conn_mock, hook):
        method_mock = self.mock_endpoint(get_conn_mock).patch
        method_mock.return_value.execute.return_value = "value"
        result = hook.patch_instance(
            instance_name=INSTANCE_NAME,
            instance=INSTANCE,
            update_mask="instance.name",
            location=LOCATION,
            project_id=PROJECT_ID,
        )

        assert result == "value"
        method_mock.assert_called_once_with(
            name=hook._name(PROJECT_ID, LOCATION, INSTANCE_NAME),
            body=INSTANCE,
            updateMask="instance.name",
        )

    @mock.patch(HOOK_STR.format("DataFusionHook.get_conn"))
    def test_get_instance(self, get_conn_mock, hook):
        method_mock = self.mock_endpoint(get_conn_mock).get
        method_mock.return_value.execute.return_value = "value"
        result = hook.get_instance(instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID)

        assert result == "value"
        method_mock.assert_called_once_with(name=hook._name(PROJECT_ID, LOCATION, INSTANCE_NAME))

    @mock.patch("google.auth.transport.requests.Request")
    @mock.patch(HOOK_STR.format("DataFusionHook._get_credentials"))
    def test_cdap_request(self, get_credentials_mock, mock_request, hook):
        url = "test_url"
        headers = {"Content-Type": "application/json"}
        method = "POST"
        request = mock_request.return_value
        request.return_value = mock.MagicMock()
        body = {"data": "value"}

        result = hook._cdap_request(url=url, method=method, body=body)
        mock_request.assert_called_once_with()
        get_credentials_mock.assert_called_once_with()
        get_credentials_mock.return_value.before_request.assert_called_once_with(
            request=request, method=method, url=url, headers=headers
        )
        request.assert_called_once_with(method=method, url=url, headers=headers, body=json.dumps(body))
        assert result == request.return_value

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_create_pipeline(self, mock_request, hook):
        mock_request.return_value.status = 200
        hook.create_pipeline(pipeline_name=PIPELINE_NAME, pipeline=PIPELINE, instance_url=INSTANCE_URL)
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}",
            method="PUT",
            body=PIPELINE,
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_delete_pipeline(self, mock_request, hook):
        mock_request.return_value.status = 200
        hook.delete_pipeline(pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL)
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}",
            method="DELETE",
            body=None,
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_list_pipelines(self, mock_request, hook):
        data = {"data": "test"}
        mock_request.return_value.status = 200
        mock_request.return_value.data = json.dumps(data)
        result = hook.list_pipelines(instance_url=INSTANCE_URL)
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps", method="GET", body=None
        )
        assert result == data

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    @mock.patch(HOOK_STR.format("DataFusionHook.wait_for_pipeline_state"))
    def test_start_pipeline(self, mock_wait_for_pipeline_state, mock_request, hook):
        run_id = 1234
        mock_request.return_value = mock.MagicMock(status=200, data=f'[{{"runId":{run_id}}}]')

        hook.start_pipeline(pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL, runtime_args=RUNTIME_ARGS)
        body = [
            {
                "appId": PIPELINE_NAME,
                "programType": "workflow",
                "programId": "DataPipelineWorkflow",
                "runtimeargs": RUNTIME_ARGS,
            }
        ]
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/start", method="POST", body=body
        )
        mock_wait_for_pipeline_state.assert_called_once_with(
            instance_url=INSTANCE_URL,
            namespace="default",
            pipeline_name=PIPELINE_NAME,
            pipeline_id=run_id,
            success_states=SUCCESS_STATES + [PipelineStates.RUNNING],
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_stop_pipeline(self, mock_request, hook):
        mock_request.return_value.status = 200
        hook.stop_pipeline(pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL)
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}/"
            f"workflows/DataPipelineWorkflow/stop",
            method="POST",
        )
