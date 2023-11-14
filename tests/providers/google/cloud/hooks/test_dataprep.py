#
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
import os
from unittest import mock
from unittest.mock import patch

import pytest
from requests import HTTPError
from tenacity import RetryError

from airflow.providers.google.cloud.hooks.dataprep import GoogleDataprepHook

JOB_ID = 1234567
RECIPE_ID = 1234567
TOKEN = "1111"
EXTRA = {"token": TOKEN}
EMBED = ""
INCLUDE_DELETED = False
DATA = {"wrangledDataset": {"id": RECIPE_ID}}
URL_BASE = "https://api.clouddataprep.com"
URL_JOB_GROUPS = URL_BASE + "/v4/jobGroups"
URL_IMPORTED_DATASETS = URL_BASE + "/v4/importedDatasets"
URL_WRANGLED_DATASETS = URL_BASE + "/v4/wrangledDatasets"
URL_OUTPUT_OBJECTS = URL_BASE + "/v4/outputObjects"
URL_WRITE_SETTINGS = URL_BASE + "/v4/writeSettings"

# needed to ignore MyPy badly detecting HTTPError as requiring response parameter
# HTTPError will fall-back to None when parameter is not present.
# mypy: disable-error-code="call-arg"


class TestGoogleDataprepHook:
    def setup_method(self):
        with mock.patch("airflow.hooks.base.BaseHook.get_connection") as conn:
            conn.return_value.extra_dejson = EXTRA
            self.hook = GoogleDataprepHook(dataprep_conn_id="dataprep_default")
        self._imported_dataset_id = 12345
        self._create_imported_dataset_body_request = {
            "uri": "gs://test/uri",
            "name": "test_name",
        }
        self._create_wrangled_dataset_body_request = {
            "importedDataset": {"id": "test_dataset_id"},
            "flow": {"id": "test_flow_id"},
            "name": "test_dataset_name",
        }
        self._create_output_object_body_request = {
            "execution": "dataflow",
            "profiler": False,
            "flowNodeId": "test_flow_node_id",
        }
        self._create_write_settings_body_request = {
            "path": "gs://test/path",
            "action": "create",
            "format": "csv",
            "outputObjectId": "test_output_object_id",
        }
        self._expected_create_imported_dataset_hook_data = json.dumps(
            self._create_imported_dataset_body_request
        )
        self._expected_create_wrangled_dataset_hook_data = json.dumps(
            self._create_wrangled_dataset_body_request
        )
        self._expected_create_output_object_hook_data = json.dumps(self._create_output_object_body_request)
        self._expected_create_write_settings_hook_data = json.dumps(self._create_write_settings_body_request)

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.get")
    def test_get_jobs_for_job_group_should_be_called_once_with_params(self, mock_get_request):
        self.hook.get_jobs_for_job_group(JOB_ID)
        mock_get_request.assert_called_once_with(
            f"{URL_JOB_GROUPS}/{JOB_ID}/jobs",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}"},
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_get_jobs_for_job_group_should_pass_after_retry(self, mock_get_request):
        self.hook.get_jobs_for_job_group(JOB_ID)
        assert mock_get_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_get_jobs_for_job_group_should_not_retry_after_success(self, mock_get_request):
        self.hook.get_jobs_for_job_group.retry.sleep = mock.Mock()
        self.hook.get_jobs_for_job_group(JOB_ID)
        assert mock_get_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), mock.MagicMock()],
    )
    def test_get_jobs_for_job_group_should_retry_after_four_errors(self, mock_get_request):
        self.hook.get_jobs_for_job_group.retry.sleep = mock.Mock()
        self.hook.get_jobs_for_job_group(JOB_ID)
        assert mock_get_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_get_jobs_for_job_group_raise_error_after_five_calls(self, mock_get_request):
        with pytest.raises(RetryError) as ctx:
            self.hook.get_jobs_for_job_group.retry.sleep = mock.Mock()
            self.hook.get_jobs_for_job_group(JOB_ID)
        assert "HTTPError" in str(ctx.value)
        assert mock_get_request.call_count == 5

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.get")
    def test_get_job_group_should_be_called_once_with_params(self, mock_get_request):
        self.hook.get_job_group(JOB_ID, EMBED, INCLUDE_DELETED)
        mock_get_request.assert_called_once_with(
            f"{URL_JOB_GROUPS}/{JOB_ID}",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            },
            params={"embed": "", "includeDeleted": False},
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_get_job_group_should_pass_after_retry(self, mock_get_request):
        self.hook.get_job_group(JOB_ID, EMBED, INCLUDE_DELETED)
        assert mock_get_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_get_job_group_should_not_retry_after_success(self, mock_get_request):
        self.hook.get_job_group.retry.sleep = mock.Mock()
        self.hook.get_job_group(JOB_ID, EMBED, INCLUDE_DELETED)
        assert mock_get_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            mock.MagicMock(),
        ],
    )
    def test_get_job_group_should_retry_after_four_errors(self, mock_get_request):
        self.hook.get_job_group.retry.sleep = mock.Mock()
        self.hook.get_job_group(JOB_ID, EMBED, INCLUDE_DELETED)
        assert mock_get_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_get_job_group_raise_error_after_five_calls(self, mock_get_request):
        with pytest.raises(RetryError) as ctx:
            self.hook.get_job_group.retry.sleep = mock.Mock()
            self.hook.get_job_group(JOB_ID, EMBED, INCLUDE_DELETED)
        assert "HTTPError" in str(ctx.value)
        assert mock_get_request.call_count == 5

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.post")
    def test_run_job_group_should_be_called_once_with_params(self, mock_get_request):
        self.hook.run_job_group(body_request=DATA)
        mock_get_request.assert_called_once_with(
            f"{URL_JOB_GROUPS}",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            },
            data=json.dumps(DATA),
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_run_job_group_should_pass_after_retry(self, mock_get_request):
        self.hook.run_job_group(body_request=DATA)
        assert mock_get_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_run_job_group_should_not_retry_after_success(self, mock_get_request):
        self.hook.run_job_group.retry.sleep = mock.Mock()
        self.hook.run_job_group(body_request=DATA)
        assert mock_get_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            mock.MagicMock(),
        ],
    )
    def test_run_job_group_should_retry_after_four_errors(self, mock_get_request):
        self.hook.run_job_group.retry.sleep = mock.Mock()
        self.hook.run_job_group(body_request=DATA)
        assert mock_get_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_run_job_group_raise_error_after_five_calls(self, mock_get_request):
        with pytest.raises(RetryError) as ctx:
            self.hook.run_job_group.retry.sleep = mock.Mock()
            self.hook.run_job_group(body_request=DATA)
        assert "HTTPError" in str(ctx.value)
        assert mock_get_request.call_count == 5

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.get")
    def test_get_job_group_status_should_be_called_once_with_params(self, mock_get_request):
        self.hook.get_job_group_status(job_group_id=JOB_ID)
        mock_get_request.assert_called_once_with(
            f"{URL_JOB_GROUPS}/{JOB_ID}/status",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            },
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_get_job_group_status_should_pass_after_retry(self, mock_get_request):
        self.hook.get_job_group_status(job_group_id=JOB_ID)
        assert mock_get_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_get_job_group_status_retry_after_success(self, mock_get_request):
        self.hook.run_job_group.retry.sleep = mock.Mock()
        self.hook.get_job_group_status(job_group_id=JOB_ID)
        assert mock_get_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            mock.MagicMock(),
        ],
    )
    def test_get_job_group_status_four_errors(self, mock_get_request):
        self.hook.run_job_group.retry.sleep = mock.Mock()
        self.hook.get_job_group_status(job_group_id=JOB_ID)
        assert mock_get_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_get_job_group_status_five_calls(self, mock_get_request):
        with pytest.raises(RetryError) as ctx:
            self.hook.get_job_group_status.retry.sleep = mock.Mock()
            self.hook.get_job_group_status(job_group_id=JOB_ID)
        assert "HTTPError" in str(ctx.value)
        assert mock_get_request.call_count == 5

    @pytest.mark.parametrize(
        "uri",
        [
            pytest.param("a://?extra__dataprep__token=abc&extra__dataprep__base_url=abc", id="prefix"),
            pytest.param("a://?token=abc&base_url=abc", id="no-prefix"),
        ],
    )
    def test_conn_extra_backcompat_prefix(self, uri):
        with patch.dict(os.environ, {"AIRFLOW_CONN_MY_CONN": uri}):
            hook = GoogleDataprepHook("my_conn")
            assert hook._token == "abc"
            assert hook._base_url == "abc"

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.post")
    def test_create_imported_dataset_should_be_called_once_with_params(self, mock_post_request):
        self.hook.create_imported_dataset(body_request=self._create_imported_dataset_body_request)
        mock_post_request.assert_called_once_with(
            URL_IMPORTED_DATASETS,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            },
            data=self._expected_create_imported_dataset_hook_data,
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_create_imported_dataset_should_pass_after_retry(self, mock_post_request):
        self.hook.create_imported_dataset(body_request=self._create_imported_dataset_body_request)
        assert mock_post_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_create_imported_dataset_retry_after_success(self, mock_post_request):
        self.hook.create_imported_dataset.retry.sleep = mock.Mock()
        self.hook.create_imported_dataset(body_request=self._create_imported_dataset_body_request)
        assert mock_post_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            mock.MagicMock(),
        ],
    )
    def test_create_imported_dataset_four_errors(self, mock_post_request):
        self.hook.create_imported_dataset.retry.sleep = mock.Mock()
        self.hook.create_imported_dataset(body_request=self._create_imported_dataset_body_request)
        assert mock_post_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_create_imported_dataset_five_calls(self, mock_post_request):
        with pytest.raises(RetryError) as ctx:
            self.hook.create_imported_dataset.retry.sleep = mock.Mock()
            self.hook.create_imported_dataset(body_request=self._create_imported_dataset_body_request)
        assert "HTTPError" in str(ctx.value)
        assert mock_post_request.call_count == 5

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.post")
    def test_create_wrangled_dataset_should_be_called_once_with_params(self, mock_post_request):
        self.hook.create_wrangled_dataset(body_request=self._create_wrangled_dataset_body_request)
        mock_post_request.assert_called_once_with(
            URL_WRANGLED_DATASETS,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            },
            data=self._expected_create_wrangled_dataset_hook_data,
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_create_wrangled_dataset_should_pass_after_retry(self, mock_post_request):
        self.hook.create_wrangled_dataset(body_request=self._create_wrangled_dataset_body_request)
        assert mock_post_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_create_wrangled_dataset_retry_after_success(self, mock_post_request):
        self.hook.create_wrangled_dataset.retry.sleep = mock.Mock()
        self.hook.create_wrangled_dataset(body_request=self._create_wrangled_dataset_body_request)
        assert mock_post_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            mock.MagicMock(),
        ],
    )
    def test_create_wrangled_dataset_four_errors(self, mock_post_request):
        self.hook.create_wrangled_dataset.retry.sleep = mock.Mock()
        self.hook.create_wrangled_dataset(body_request=self._create_wrangled_dataset_body_request)
        assert mock_post_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_create_wrangled_dataset_five_calls(self, mock_post_request):
        with pytest.raises(RetryError) as ctx:
            self.hook.create_wrangled_dataset.retry.sleep = mock.Mock()
            self.hook.create_wrangled_dataset(body_request=self._create_wrangled_dataset_body_request)
        assert "HTTPError" in str(ctx.value)
        assert mock_post_request.call_count == 5

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.post")
    def test_create_output_object_should_be_called_once_with_params(self, mock_post_request):
        self.hook.create_output_object(body_request=self._create_output_object_body_request)
        mock_post_request.assert_called_once_with(
            URL_OUTPUT_OBJECTS,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            },
            data=self._expected_create_output_object_hook_data,
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_create_output_objects_should_pass_after_retry(self, mock_post_request):
        self.hook.create_output_object(body_request=self._create_output_object_body_request)
        assert mock_post_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_create_output_objects_retry_after_success(self, mock_post_request):
        self.hook.create_output_object.retry.sleep = mock.Mock()
        self.hook.create_output_object(body_request=self._create_output_object_body_request)
        assert mock_post_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            mock.MagicMock(),
        ],
    )
    def test_create_output_objects_four_errors(self, mock_post_request):
        self.hook.create_output_object.retry.sleep = mock.Mock()
        self.hook.create_output_object(body_request=self._create_output_object_body_request)
        assert mock_post_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_create_output_objects_five_calls(self, mock_post_request):
        with pytest.raises(RetryError) as ctx:
            self.hook.create_output_object.retry.sleep = mock.Mock()
            self.hook.create_output_object(body_request=self._create_output_object_body_request)
        assert "HTTPError" in str(ctx.value)
        assert mock_post_request.call_count == 5

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.post")
    def test_create_write_settings_should_be_called_once_with_params(self, mock_post_request):
        self.hook.create_write_settings(body_request=self._create_write_settings_body_request)
        mock_post_request.assert_called_once_with(
            URL_WRITE_SETTINGS,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            },
            data=self._expected_create_write_settings_hook_data,
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_create_write_settings_should_pass_after_retry(self, mock_post_request):
        self.hook.create_write_settings(body_request=self._create_write_settings_body_request)
        assert mock_post_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_create_write_settings_retry_after_success(self, mock_post_request):
        self.hook.create_write_settings.retry.sleep = mock.Mock()
        self.hook.create_write_settings(body_request=self._create_write_settings_body_request)
        assert mock_post_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            mock.MagicMock(),
        ],
    )
    def test_create_write_settings_four_errors(self, mock_post_request):
        self.hook.create_write_settings.retry.sleep = mock.Mock()
        self.hook.create_write_settings(body_request=self._create_write_settings_body_request)
        assert mock_post_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_create_write_settings_five_calls(self, mock_post_request):
        with pytest.raises(RetryError) as ctx:
            self.hook.create_write_settings.retry.sleep = mock.Mock()
            self.hook.create_write_settings(body_request=self._create_write_settings_body_request)
        assert "HTTPError" in str(ctx.value)
        assert mock_post_request.call_count == 5

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.delete")
    def test_delete_imported_dataset_should_be_called_once_with_params(self, mock_delete_request):
        self.hook.delete_imported_dataset(dataset_id=self._imported_dataset_id)
        mock_delete_request.assert_called_once_with(
            f"{URL_IMPORTED_DATASETS}/{self._imported_dataset_id}",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            },
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.delete",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_delete_imported_dataset_should_pass_after_retry(self, mock_delete_request):
        self.hook.delete_imported_dataset(dataset_id=self._imported_dataset_id)
        assert mock_delete_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.delete",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_delete_imported_dataset_retry_after_success(self, mock_delete_request):
        self.hook.delete_imported_dataset.retry.sleep = mock.Mock()
        self.hook.delete_imported_dataset(dataset_id=self._imported_dataset_id)
        assert mock_delete_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.delete",
        side_effect=[
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            mock.MagicMock(),
        ],
    )
    def test_delete_imported_dataset_four_errors(self, mock_delete_request):
        self.hook.delete_imported_dataset.retry.sleep = mock.Mock()
        self.hook.delete_imported_dataset(dataset_id=self._imported_dataset_id)
        assert mock_delete_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.delete",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_delete_imported_dataset_five_calls(self, mock_delete_request):
        with pytest.raises(RetryError) as ctx:
            self.hook.delete_imported_dataset.retry.sleep = mock.Mock()
            self.hook.delete_imported_dataset(dataset_id=self._imported_dataset_id)
        assert "HTTPError" in str(ctx.value)
        assert mock_delete_request.call_count == 5


class TestGoogleDataprepFlowPathHooks:
    _url = "https://api.clouddataprep.com/v4/flows"

    def setup_method(self):
        self._flow_id = 1234567
        self._create_flow_body_request = {
            "name": "test_name",
            "description": "Test description",
        }
        self._expected_copy_flow_hook_data = json.dumps(
            {
                "name": "",
                "description": "",
                "copyDatasources": False,
            }
        )
        self._expected_run_flow_hook_data = json.dumps({})
        self._expected_create_flow_hook_data = json.dumps(
            {
                "name": "test_name",
                "description": "Test description",
            }
        )
        with mock.patch("airflow.hooks.base.BaseHook.get_connection") as conn:
            conn.return_value.extra_dejson = EXTRA
            self.hook = GoogleDataprepHook(dataprep_conn_id="dataprep_default")

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.post")
    def test_create_flow_should_be_called_once_with_params(self, mock_post_request):
        self.hook.create_flow(body_request=self._create_flow_body_request)
        mock_post_request.assert_called_once_with(
            self._url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            },
            data=self._expected_create_flow_hook_data,
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_create_flow_should_pass_after_retry(self, mock_post_request):
        self.hook.create_flow(body_request=self._create_flow_body_request)
        assert mock_post_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_create_flow_should_not_retry_after_success(self, mock_post_request):
        self.hook.create_flow.retry.sleep = mock.Mock()
        self.hook.create_flow(body_request=self._create_flow_body_request)
        assert mock_post_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            mock.MagicMock(),
        ],
    )
    def test_create_flow_should_retry_after_four_errors(self, mock_post_request):
        self.hook.create_flow.retry.sleep = mock.Mock()
        self.hook.create_flow(body_request=self._create_flow_body_request)
        assert mock_post_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_create_flow_raise_error_after_five_calls(self, mock_post_request):
        with pytest.raises(RetryError) as ctx:
            self.hook.create_flow.retry.sleep = mock.Mock()
            self.hook.create_flow(body_request=self._create_flow_body_request)
        assert "HTTPError" in str(ctx.value)
        assert mock_post_request.call_count == 5

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.post")
    def test_copy_flow_should_be_called_once_with_params(self, mock_get_request):
        self.hook.copy_flow(
            flow_id=self._flow_id,
        )
        mock_get_request.assert_called_once_with(
            f"{self._url}/{self._flow_id}/copy",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            },
            data=self._expected_copy_flow_hook_data,
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_copy_flow_should_pass_after_retry(self, mock_get_request):
        self.hook.copy_flow(flow_id=self._flow_id)
        assert mock_get_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_copy_flow_should_not_retry_after_success(self, mock_get_request):
        self.hook.copy_flow.retry.sleep = mock.Mock()
        self.hook.copy_flow(flow_id=self._flow_id)
        assert mock_get_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            mock.MagicMock(),
        ],
    )
    def test_copy_flow_should_retry_after_four_errors(self, mock_get_request):
        self.hook.copy_flow.retry.sleep = mock.Mock()
        self.hook.copy_flow(flow_id=self._flow_id)
        assert mock_get_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_copy_flow_raise_error_after_five_calls(self, mock_get_request):
        with pytest.raises(RetryError) as ctx:
            self.hook.copy_flow.retry.sleep = mock.Mock()
            self.hook.copy_flow(flow_id=self._flow_id)
        assert "HTTPError" in str(ctx.value)
        assert mock_get_request.call_count == 5

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.delete")
    def test_delete_flow_should_be_called_once_with_params(self, mock_get_request):
        self.hook.delete_flow(
            flow_id=self._flow_id,
        )
        mock_get_request.assert_called_once_with(
            f"{self._url}/{self._flow_id}",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            },
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.delete",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_delete_flow_should_pass_after_retry(self, mock_get_request):
        self.hook.delete_flow(flow_id=self._flow_id)
        assert mock_get_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.delete",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_delete_flow_should_not_retry_after_success(self, mock_get_request):
        self.hook.delete_flow.retry.sleep = mock.Mock()
        self.hook.delete_flow(flow_id=self._flow_id)
        assert mock_get_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.delete",
        side_effect=[
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            mock.MagicMock(),
        ],
    )
    def test_delete_flow_should_retry_after_four_errors(self, mock_get_request):
        self.hook.delete_flow.retry.sleep = mock.Mock()
        self.hook.delete_flow(flow_id=self._flow_id)
        assert mock_get_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.delete",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_delete_flow_raise_error_after_five_calls(self, mock_get_request):
        with pytest.raises(RetryError) as ctx:
            self.hook.delete_flow.retry.sleep = mock.Mock()
            self.hook.delete_flow(flow_id=self._flow_id)
        assert "HTTPError" in str(ctx.value)
        assert mock_get_request.call_count == 5

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.post")
    def test_run_flow_should_be_called_once_with_params(self, mock_get_request):
        self.hook.run_flow(
            flow_id=self._flow_id,
            body_request={},
        )
        mock_get_request.assert_called_once_with(
            f"{self._url}/{self._flow_id}/run",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            },
            data=self._expected_run_flow_hook_data,
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_run_flow_should_pass_after_retry(self, mock_get_request):
        self.hook.run_flow(
            flow_id=self._flow_id,
            body_request={},
        )
        assert mock_get_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_run_flow_should_not_retry_after_success(self, mock_get_request):
        self.hook.run_flow.retry.sleep = mock.Mock()
        self.hook.run_flow(
            flow_id=self._flow_id,
            body_request={},
        )
        assert mock_get_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[
            HTTPError(),
            HTTPError(),
            HTTPError(),
            HTTPError(),
            mock.MagicMock(),
        ],
    )
    def test_run_flow_should_retry_after_four_errors(self, mock_get_request):
        self.hook.run_flow.retry.sleep = mock.Mock()
        self.hook.run_flow(
            flow_id=self._flow_id,
            body_request={},
        )
        assert mock_get_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_run_flow_raise_error_after_five_calls(self, mock_get_request):
        with pytest.raises(RetryError) as ctx:
            self.hook.run_flow.retry.sleep = mock.Mock()
            self.hook.run_flow(
                flow_id=self._flow_id,
                body_request={},
            )
        assert "HTTPError" in str(ctx.value)
        assert mock_get_request.call_count == 5
