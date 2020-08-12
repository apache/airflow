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
from unittest import mock

import pytest
from mock import patch
from requests import HTTPError
from tenacity import RetryError

from airflow.providers.google.cloud.hooks import dataprep

JOB_ID = 1234567
URL = "https://api.clouddataprep.com/v4/jobGroups"
TOKEN = "1111"
EXTRA = {"token": TOKEN}


@pytest.fixture(scope="class")
def mock_hook():
    with mock.patch("airflow.hooks.base_hook.BaseHook.get_connection") as conn:
        hook = dataprep.GoogleDataprepHook(dataprep_conn_id="dataprep_conn_id")
        conn.return_value.extra_dejson = EXTRA
        yield hook


class TestGoogleDataprepHook:
    def test_get_token(self, mock_hook):
        assert mock_hook._token == TOKEN

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.get")
    def test_mock_should_be_called_once_with_params(self, mock_get_request, mock_hook):
        mock_hook.get_jobs_for_job_group(job_id=JOB_ID)
        mock_get_request.assert_called_once_with(
            f"{URL}/{JOB_ID}/jobs",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            },
        )

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), mock.MagicMock()],
    )
    def test_should_pass_after_retry(self, mock_get_request, mock_hook):
        mock_hook.get_jobs_for_job_group(JOB_ID)
        assert mock_get_request.call_count == 2

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[mock.MagicMock(), HTTPError()],
    )
    def test_should_not_retry_after_success(self, mock_get_request, mock_hook):
        mock_hook.get_jobs_for_job_group.retry.sleep = mock.Mock()  # pylint: disable=no-member
        mock_hook.get_jobs_for_job_group(JOB_ID)
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
    def test_should_retry_after_four_errors(self, mock_get_request, mock_hook):
        mock_hook.get_jobs_for_job_group.retry.sleep = mock.Mock()  # pylint: disable=no-member
        mock_hook.get_jobs_for_job_group(JOB_ID)
        assert mock_get_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_raise_error_after_five_calls(self, mock_get_request, mock_hook):
        with pytest.raises(RetryError) as err:
            mock_hook.get_jobs_for_job_group.retry.sleep = mock.Mock()  # pylint: disable=no-member
            mock_hook.get_jobs_for_job_group(JOB_ID)
        assert "HTTPError" in str(err)
        assert mock_get_request.call_count == 5
