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
import json
import unittest
from unittest import mock

import pytest
from mock import patch
from requests import HTTPError
from tenacity import RetryError

from airflow.providers.google.cloud.hooks import dataprep

JOB_ID = 1234567
RECIPE_ID = 1234567
TOKEN = "1111"
EXTRA = {"extra__dataprep__token": TOKEN}
EMBED = ""
INCLUDE_DELETED = False
DATA = json.dumps({"wrangledDataset": {"id": RECIPE_ID}})
URL = "https://api.clouddataprep.com/v4/jobGroups"


class TestGoogleDataprepHook(unittest.TestCase):
    def setUp(self):
        with mock.patch("airflow.hooks.base_hook.BaseHook.get_connection") as conn:
            conn.return_value.extra_dejson = EXTRA
            self.hook = dataprep.GoogleDataprepHook(dataprep_conn_id="dataprep_default")

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
        # pylint: disable=no-member
        self.hook.get_jobs_for_job_group.retry.sleep = mock.Mock()
        self.hook.get_jobs_for_job_group(JOB_ID)
        assert mock_get_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), mock.MagicMock()],
    )
    def test_get_jobs_for_job_group_should_retry_after_four_errors(self, mock_get_request):
        # pylint: disable=no-member
        self.hook.get_jobs_for_job_group.retry.sleep = mock.Mock()
        self.hook.get_jobs_for_job_group(JOB_ID)
        assert mock_get_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_get_jobs_for_job_group_raise_error_after_five_calls(self, mock_get_request):
        with pytest.raises(RetryError) as err:
            # pylint: disable=no-member
            self.hook.get_jobs_for_job_group.retry.sleep = mock.Mock()
            self.hook.get_jobs_for_job_group(JOB_ID)
        assert "HTTPError" in str(err)
        assert mock_get_request.call_count == 5

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.get")
    def test_get_job_group_should_be_called_once_with_params(self, mock_get_request):
        self.hook.get_job_group(JOB_ID, EMBED, INCLUDE_DELETED)
        mock_get_request.assert_called_once_with(
            f"{URL}/{JOB_ID}",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}",},
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
        self.hook.get_job_group.retry.sleep = mock.Mock()  # pylint: disable=no-member
        self.hook.get_job_group(JOB_ID, EMBED, INCLUDE_DELETED)
        assert mock_get_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), mock.MagicMock(),],
    )
    def test_get_job_group_should_retry_after_four_errors(self, mock_get_request):
        self.hook.get_job_group.retry.sleep = mock.Mock()  # pylint: disable=no-member
        self.hook.get_job_group(JOB_ID, EMBED, INCLUDE_DELETED)
        assert mock_get_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.get",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_get_job_group_raise_error_after_five_calls(self, mock_get_request):
        with pytest.raises(RetryError) as err:
            # pylint: disable=no-member
            self.hook.get_job_group.retry.sleep = mock.Mock()
            self.hook.get_job_group(JOB_ID, EMBED, INCLUDE_DELETED)
        assert "HTTPError" in str(err)
        assert mock_get_request.call_count == 5

    @patch("airflow.providers.google.cloud.hooks.dataprep.requests.post")
    def test_run_job_group_should_be_called_once_with_params(self, mock_get_request):
        data = '"{\\"wrangledDataset\\": {\\"id\\": 1234567}}"'
        self.hook.run_job_group(body_request=DATA)
        mock_get_request.assert_called_once_with(
            f"{URL}",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}",},
            data=data,
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
        self.hook.run_job_group.retry.sleep = mock.Mock()  # pylint: disable=no-member
        self.hook.run_job_group(body_request=DATA)
        assert mock_get_request.call_count == 1

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), mock.MagicMock(),],
    )
    def test_run_job_group_should_retry_after_four_errors(self, mock_get_request):
        self.hook.run_job_group.retry.sleep = mock.Mock()  # pylint: disable=no-member
        self.hook.run_job_group(body_request=DATA)
        assert mock_get_request.call_count == 5

    @patch(
        "airflow.providers.google.cloud.hooks.dataprep.requests.post",
        side_effect=[HTTPError(), HTTPError(), HTTPError(), HTTPError(), HTTPError()],
    )
    def test_run_job_group_raise_error_after_five_calls(self, mock_get_request):
        with pytest.raises(RetryError) as err:
            # pylint: disable=no-member
            self.hook.run_job_group.retry.sleep = mock.Mock()
            self.hook.run_job_group(body_request=DATA)
        assert "HTTPError" in str(err)
        assert mock_get_request.call_count == 5
