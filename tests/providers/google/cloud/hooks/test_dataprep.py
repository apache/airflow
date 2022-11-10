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
from unittest import TestCase, mock
from unittest.mock import patch

import pytest
from pytest import param
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
URL = "https://api.clouddataprep.com/v4/jobGroups"


class TestGoogleDataprepHook:
    def setup(self):
        with mock.patch("airflow.hooks.base.BaseHook.get_connection") as conn:
            conn.return_value.extra_dejson = EXTRA
            self.hook = GoogleDataprepHook(dataprep_conn_id="dataprep_default")

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.get")
    def test_get_jobs_for_job_group_should_be_called_once_with_params(self, mock_get_request):
        self.hook.get_jobs_for_job_group(JOB_ID)
        mock_get_request.assert_called_once_with(
            f"{URL}/{JOB_ID}/jobs",
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
            f"{URL}/{JOB_ID}",
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
            f"{URL}",
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
            f"{URL}/{JOB_ID}/status",
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
            param("a://?extra__dataprep__token=abc&extra__dataprep__base_url=abc", id="prefix"),
            param("a://?token=abc&base_url=abc", id="no-prefix"),
        ],
    )
    def test_conn_extra_backcompat_prefix(self, uri):
        with patch.dict(os.environ, {"AIRFLOW_CONN_MY_CONN": uri}):
            hook = GoogleDataprepHook("my_conn")
            assert hook._token == "abc"
            assert hook._base_url == "abc"


class TestGoogleDataprepFlowPathHooks(TestCase):
    _url = "https://api.clouddataprep.com/v4/flows"

    def setUp(self) -> None:
        self._flow_id = 1234567
        self._expected_copy_flow_hook_data = json.dumps(
            {
                "name": "",
                "description": "",
                "copyDatasources": False,
            }
        )
        self._expected_run_flow_hook_data = json.dumps({})
        with mock.patch("airflow.hooks.base.BaseHook.get_connection") as conn:
            conn.return_value.extra_dejson = EXTRA
            self.hook = GoogleDataprepHook(dataprep_conn_id="dataprep_default")

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
