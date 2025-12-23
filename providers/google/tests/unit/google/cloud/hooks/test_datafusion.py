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

import json
import logging
from unittest import mock

import aiohttp
import google.auth.transport
import pytest
from aiohttp.helpers import TimerNoop
from yarl import URL

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.datafusion import DataFusionAsyncHook, DataFusionHook
from airflow.providers.google.cloud.utils.datafusion import DataFusionPipelineType

from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

API_VERSION = "v1beta1"
GCP_CONN_ID = "google_cloud_default"
HOOK_STR = "airflow.providers.google.cloud.hooks.datafusion.{}"
LOCATION = "test-location"
INSTANCE_NAME = "airflow-test-instance"
INSTANCE_URL = "http://datafusion.instance.com"
INSTANCE = {"type": "BASIC", "displayName": INSTANCE_NAME}
NAMESPACE = "default"
PROJECT_ID = "test_project_id"
PIPELINE_NAME = "shrubberyPipeline"
PIPELINE_ID = "123"
PIPELINE = {"test": "pipeline"}
RUNTIME_ARGS = {"arg1": "a", "arg2": "b"}
CONSTRUCTED_PIPELINE_URL = (
    f"{INSTANCE_URL}/v3/namespaces/{NAMESPACE}/apps/{PIPELINE_NAME}"
    f"/workflows/DataPipelineWorkflow/runs/{PIPELINE_ID}"
)
CONSTRUCTED_PIPELINE_URL_GET = (
    f"https://{INSTANCE_NAME}-{PROJECT_ID}-dot-eun1.datafusion."
    f"googleusercontent.com/api/v3/namespaces/{NAMESPACE}/apps/{PIPELINE_NAME}"
    f"/workflows/DataPipelineWorkflow/runs/{PIPELINE_ID}"
)
CONSTRUCTED_PIPELINE_STREAM_URL_GET = (
    f"https://{INSTANCE_NAME}-{PROJECT_ID}-dot-eun1.datafusion."
    f"googleusercontent.com/api/v3/namespaces/{NAMESPACE}/apps/{PIPELINE_NAME}"
    f"/apsrkss/DataStreamsSparkStreaming/runs/{PIPELINE_ID}"
)


class MockResponse:
    def __init__(self, status, data=None):
        self.status = status
        self.data = data


@pytest.fixture
def hook():
    with mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    ):
        yield DataFusionHook(gcp_conn_id=GCP_CONN_ID)


@pytest.fixture
def hook_async():
    with mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseAsyncHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    ):
        yield DataFusionAsyncHook()


def session():
    return mock.Mock()


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

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_get_instance_artifacts(self, mock_request, hook):
        scope = "SYSTEM"
        artifact = {
            "name": "test-artifact",
            "version": "1.2.3",
            "scope": scope,
        }
        mock_request.return_value = mock.MagicMock(status=200, data=json.dumps([artifact]))

        hook.get_instance_artifacts(instance_url=INSTANCE_URL, scope=scope)

        mock_request.assert_called_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/artifacts",
            method="GET",
            params={"scope": scope},
        )

    @mock.patch("google.auth.transport.requests.Request")
    @mock.patch(HOOK_STR.format("DataFusionHook.get_credentials"))
    def test_cdap_request(self, get_credentials_mock, mock_request, hook):
        url = "test_url"
        headers = {"Content-Type": "application/json"}
        method = "POST"
        request = mock_request.return_value
        request.return_value = mock.MagicMock()
        body = {"data": "value"}
        params = {"param_key": "param_value"}

        result = hook._cdap_request(url=url, method=method, body=body, params=params)
        mock_request.assert_called_once_with()
        get_credentials_mock.assert_called_once_with()
        get_credentials_mock.return_value.before_request.assert_called_once_with(
            request=request, method=method, url=url, headers=headers
        )
        request.assert_called_once_with(
            method=method, url=url, headers=headers, body=json.dumps(body), params=params
        )
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
    def test_create_pipeline_should_fail_if_empty_data_response(self, mock_request, hook):
        mock_request.return_value.status = 200
        mock_request.return_value.data = None
        with pytest.raises(
            AirflowException,
            match=r"Empty response received. Please, check for possible root causes "
            r"of this behavior either in DAG code or on Cloud DataFusion side",
        ):
            hook.create_pipeline(pipeline_name=PIPELINE_NAME, pipeline=PIPELINE, instance_url=INSTANCE_URL)
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}",
            method="PUT",
            body=PIPELINE,
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_create_pipeline_should_fail_if_status_not_200(self, mock_request, hook):
        mock_request.return_value.status = 404
        with pytest.raises(AirflowException, match=r"Creating a pipeline failed with code 404"):
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
    def test_delete_pipeline_should_fail_if_empty_data_response(self, mock_request, hook):
        mock_request.return_value.status = 200
        mock_request.return_value.data = None
        with pytest.raises(
            AirflowException,
            match=r"Empty response received. Please, check for possible root causes "
            r"of this behavior either in DAG code or on Cloud DataFusion side",
        ):
            hook.delete_pipeline(pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL)
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}",
            method="DELETE",
            body=None,
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_delete_pipeline_should_fail_if_status_not_200(self, mock_request, hook):
        mock_request.return_value.status = 404
        with pytest.raises(AirflowException, match=r"Deleting a pipeline failed with code 404"):
            hook.delete_pipeline(pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL)
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}",
            method="DELETE",
            body=None,
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_delete_pipeline_should_fail_if_status_409(self, mock_request, hook, caplog):
        caplog.set_level(logging.INFO)
        mock_request.side_effect = [
            MockResponse(status=409, data="Conflict: Resource is still in use."),
            MockResponse(status=200, data="Success"),
        ]
        hook.delete_pipeline(pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL)

        assert mock_request.call_count == 2
        assert "Conflict: Resource is still in use." in caplog.text
        mock_request.assert_called_with(
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
    def test_list_pipelines_should_fail_if_empty_data_response(self, mock_request, hook):
        mock_request.return_value.status = 200
        mock_request.return_value.data = None
        with pytest.raises(
            AirflowException,
            match=r"Empty response received. Please, check for possible root causes "
            r"of this behavior either in DAG code or on Cloud DataFusion side",
        ):
            hook.list_pipelines(instance_url=INSTANCE_URL)
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps", method="GET", body=None
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_list_pipelines_should_fail_if_status_not_200(self, mock_request, hook):
        mock_request.return_value.status = 404
        with pytest.raises(AirflowException, match=r"Listing pipelines failed with code 404"):
            hook.list_pipelines(instance_url=INSTANCE_URL)
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps", method="GET", body=None
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_start_pipeline(self, mock_request, hook):
        run_id = 1234
        mock_request.return_value = mock.MagicMock(
            spec=google.auth.transport.Response, status=200, data=f'{{"runId":{run_id}}}'
        )

        result = hook.start_pipeline(
            pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL, runtime_args=RUNTIME_ARGS
        )
        assert result == str(run_id)
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}/workflows/DataPipelineWorkflow/start",
            method="POST",
            body=RUNTIME_ARGS,
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_start_pipeline_stream(self, mock_request, hook):
        run_id = "test-run-123"
        mock_request.return_value = mock.MagicMock(
            spec=google.auth.transport.Response, status=200, data=f'{{"runId":"{run_id}"}}'
        )

        result = hook.start_pipeline(
            pipeline_name=PIPELINE_NAME,
            instance_url=INSTANCE_URL,
            runtime_args=RUNTIME_ARGS,
            pipeline_type=DataFusionPipelineType.STREAM,
        )
        assert result == run_id
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}/sparks/DataStreamsSparkStreaming/start",
            method="POST",
            body=RUNTIME_ARGS,
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_start_pipeline_should_fail_if_empty_data_response(self, mock_request, hook):
        mock_request.return_value.status = 200
        mock_request.return_value.data = None
        with pytest.raises(
            AirflowException,
            match=r"Empty response received. Please, check for possible root causes "
            r"of this behavior either in DAG code or on Cloud DataFusion side",
        ):
            hook.start_pipeline(
                pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL, runtime_args=RUNTIME_ARGS
            )
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}/workflows/DataPipelineWorkflow/start",
            method="POST",
            body=RUNTIME_ARGS,
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_start_pipeline_should_fail_if_status_not_200(self, mock_request, hook):
        mock_request.return_value.status = 404
        with pytest.raises(AirflowException, match=r"Starting a pipeline failed with code 404"):
            hook.start_pipeline(
                pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL, runtime_args=RUNTIME_ARGS
            )
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}/workflows/DataPipelineWorkflow/start",
            method="POST",
            body=RUNTIME_ARGS,
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_start_pipeline_should_fail_if_no_run_id(self, mock_request, hook):
        """Test that start_pipeline fails gracefully when response doesn't contain runId."""
        error_response = '{"error": "Invalid runtime arguments"}'
        mock_request.return_value = mock.MagicMock(
            spec=google.auth.transport.Response, status=200, data=error_response
        )
        with pytest.raises(
            AirflowException,
            match=r"Failed to start pipeline 'shrubberyPipeline'. "
            r"The response does not contain a runId. Error: Invalid runtime arguments",
        ):
            hook.start_pipeline(
                pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL, runtime_args=RUNTIME_ARGS
            )
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}/workflows/DataPipelineWorkflow/start",
            method="POST",
            body=RUNTIME_ARGS,
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

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_stop_pipeline_should_fail_if_empty_data_response(self, mock_request, hook):
        mock_request.return_value.status = 200
        mock_request.return_value.data = None
        with pytest.raises(
            AirflowException,
            match=r"Empty response received. Please, check for possible root causes "
            r"of this behavior either in DAG code or on Cloud DataFusion side",
        ):
            hook.stop_pipeline(pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL)
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}/"
            f"workflows/DataPipelineWorkflow/stop",
            method="POST",
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_stop_pipeline_should_fail_if_status_not_200(self, mock_request, hook):
        mock_request.return_value.status = 404
        with pytest.raises(AirflowException, match=r"Stopping a pipeline failed with code 404"):
            hook.stop_pipeline(pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL)
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}/"
            f"workflows/DataPipelineWorkflow/stop",
            method="POST",
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_get_pipeline_workflow(self, mock_request, hook):
        run_id = 1234
        mock_request.return_value = mock.MagicMock(status=200, data=f'[{{"runId":{run_id}}}]')
        hook.get_pipeline_workflow(
            pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL, pipeline_id=PIPELINE_ID
        )
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}/"
            f"workflows/DataPipelineWorkflow/runs/{PIPELINE_ID}",
            method="GET",
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_get_pipeline_workflow_stream(self, mock_request, hook):
        run_id = 1234
        mock_request.return_value = mock.MagicMock(status=200, data=f'[{{"runId":{run_id}}}]')
        hook.get_pipeline_workflow(
            pipeline_name=PIPELINE_NAME,
            instance_url=INSTANCE_URL,
            pipeline_id=PIPELINE_ID,
            pipeline_type=DataFusionPipelineType.STREAM,
        )
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}/"
            f"sparks/DataStreamsSparkStreaming/runs/{PIPELINE_ID}",
            method="GET",
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_get_pipeline_workflow_should_fail_if_empty_data_response(self, mock_request, hook):
        mock_request.return_value.status = 200
        mock_request.return_value.data = None
        with pytest.raises(
            AirflowException,
            match=r"Empty response received. Please, check for possible root causes "
            r"of this behavior either in DAG code or on Cloud DataFusion side",
        ):
            hook.get_pipeline_workflow(
                pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL, pipeline_id=PIPELINE_ID
            )
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}/"
            f"workflows/DataPipelineWorkflow/runs/{PIPELINE_ID}",
            method="GET",
        )

    @mock.patch(HOOK_STR.format("DataFusionHook._cdap_request"))
    def test_get_pipeline_workflow_should_fail_if_status_not_200(self, mock_request, hook):
        mock_request.return_value.status = 404
        with pytest.raises(AirflowException, match=r"Retrieving a pipeline state failed with code 404"):
            hook.get_pipeline_workflow(
                pipeline_name=PIPELINE_NAME, instance_url=INSTANCE_URL, pipeline_id=PIPELINE_ID
            )
        mock_request.assert_called_once_with(
            url=f"{INSTANCE_URL}/v3/namespaces/default/apps/{PIPELINE_NAME}/"
            f"workflows/DataPipelineWorkflow/runs/{PIPELINE_ID}",
            method="GET",
        )

    @pytest.mark.parametrize(
        ("pipeline_type", "expected_program_type"),
        [
            (DataFusionPipelineType.BATCH, "workflow"),
            (DataFusionPipelineType.STREAM, "spark"),
            ("non existing value", ""),
        ],
    )
    def test_cdap_program_type(self, pipeline_type, expected_program_type):
        assert DataFusionHook.cdap_program_type(pipeline_type) == expected_program_type

    @pytest.mark.parametrize(
        ("pipeline_type", "expected_program_id"),
        [
            (DataFusionPipelineType.BATCH, "DataPipelineWorkflow"),
            (DataFusionPipelineType.STREAM, "DataStreamsSparkStreaming"),
            ("non existing value", ""),
        ],
    )
    def test_cdap_program_id(self, pipeline_type, expected_program_id):
        assert DataFusionHook.cdap_program_id(pipeline_type) == expected_program_id


class TestDataFusionHookAsynch:
    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("DataFusionAsyncHook._get_link"))
    async def test_async_get_pipeline_should_execute_successfully(self, mocked_link, hook_async):
        await hook_async.get_pipeline(
            instance_url=INSTANCE_URL,
            namespace=NAMESPACE,
            pipeline_name=PIPELINE_NAME,
            pipeline_id=PIPELINE_ID,
            session=session,
        )
        mocked_link.assert_awaited_once_with(url=CONSTRUCTED_PIPELINE_URL, session=session)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("pipeline_type", "constructed_url"),
        [
            (DataFusionPipelineType.BATCH, CONSTRUCTED_PIPELINE_URL_GET),
            (DataFusionPipelineType.STREAM, CONSTRUCTED_PIPELINE_STREAM_URL_GET),
        ],
    )
    @mock.patch(HOOK_STR.format("DataFusionAsyncHook.get_pipeline"))
    async def test_async_get_pipeline_status_completed_should_execute_successfully(
        self, mocked_get, hook_async, pipeline_type, constructed_url
    ):
        response = aiohttp.ClientResponse(
            "get",
            URL(constructed_url),
            request_info=mock.Mock(),
            writer=mock.Mock(),
            continue100=None,
            timer=TimerNoop(),
            traces=[],
            loop=mock.Mock(),
            session=None,
        )
        response.status = 200
        mocked_get.return_value = response
        mocked_get.return_value._headers = {"Authorization": "some-token"}
        mocked_get.return_value._body = b'{"status": "COMPLETED"}'

        pipeline_status = await hook_async.get_pipeline_status(
            pipeline_name=PIPELINE_NAME,
            instance_url=INSTANCE_URL,
            pipeline_id=PIPELINE_ID,
            namespace=NAMESPACE,
        )
        mocked_get.assert_awaited_once()
        assert pipeline_status == "success"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("pipeline_type", "constructed_url"),
        [
            (DataFusionPipelineType.BATCH, CONSTRUCTED_PIPELINE_URL_GET),
            (DataFusionPipelineType.STREAM, CONSTRUCTED_PIPELINE_STREAM_URL_GET),
        ],
    )
    @mock.patch(HOOK_STR.format("DataFusionAsyncHook.get_pipeline"))
    async def test_async_get_pipeline_status_running_should_execute_successfully(
        self, mocked_get, hook_async, pipeline_type, constructed_url
    ):
        """Assets that the DataFusionAsyncHook returns pending response when job is still in running state"""
        response = aiohttp.ClientResponse(
            "get",
            URL(constructed_url),
            request_info=mock.Mock(),
            writer=mock.Mock(),
            continue100=None,
            timer=TimerNoop(),
            traces=[],
            loop=mock.Mock(),
            session=None,
        )
        response.status = 200
        mocked_get.return_value = response
        mocked_get.return_value._headers = {"Authorization": "some-token"}
        mocked_get.return_value._body = b'{"status": "RUNNING"}'

        pipeline_status = await hook_async.get_pipeline_status(
            pipeline_name=PIPELINE_NAME,
            instance_url=INSTANCE_URL,
            pipeline_id=PIPELINE_ID,
            pipeline_type=pipeline_type,
            namespace=NAMESPACE,
        )
        mocked_get.assert_awaited_once()
        assert pipeline_status == "pending"

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("DataFusionAsyncHook.get_pipeline"))
    async def test_async_get_pipeline_status_os_error_should_execute_successfully(
        self, mocked_get, hook_async
    ):
        """Assets that the DataFusionAsyncHook returns a pending response when OSError is raised"""
        mocked_get.side_effect = OSError()

        pipeline_status = await hook_async.get_pipeline_status(
            pipeline_name=PIPELINE_NAME,
            instance_url=INSTANCE_URL,
            pipeline_id=PIPELINE_ID,
            namespace=NAMESPACE,
        )
        mocked_get.assert_awaited_once()
        assert pipeline_status == "pending"

    @pytest.mark.asyncio
    @mock.patch(HOOK_STR.format("DataFusionAsyncHook.get_pipeline"))
    async def test_async_get_pipeline_status_exception_should_execute_successfully(
        self, mocked_get, hook_async, caplog
    ):
        """Assets that the logging is done correctly when DataFusionAsyncHook raises Exception"""
        caplog.set_level(logging.INFO)
        mocked_get.side_effect = Exception()

        await hook_async.get_pipeline_status(
            pipeline_name=PIPELINE_NAME,
            instance_url=INSTANCE_URL,
            pipeline_id=PIPELINE_ID,
            namespace=NAMESPACE,
        )
        mocked_get.assert_awaited_once()
        assert "Retrieving pipeline status finished with errors..." in caplog.text
