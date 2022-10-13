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

import unittest
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.looker import JobStatus, LookerHook
from airflow.version import version

HOOK_PATH = "airflow.providers.google.cloud.hooks.looker.LookerHook.{}"

JOB_ID = "test-id"
TASK_ID = "test-task-id"
MODEL = "test_model"
VIEW = "test_view"
SOURCE = f'airflow:{version}'

CONN_EXTRA = {"verify_ssl": "true", "timeout": "120"}


class TestLookerHook(unittest.TestCase):
    def setUp(self):
        with mock.patch("airflow.hooks.base.BaseHook.get_connection") as conn:
            conn.return_value.extra_dejson = CONN_EXTRA
            self.hook = LookerHook(looker_conn_id="test")

    @mock.patch(HOOK_PATH.format("pdt_build_status"))
    def test_wait_for_job(self, mock_pdt_build_status):
        # replace pdt_build_status invocation with mock status
        mock_pdt_build_status.side_effect = [
            {'status': JobStatus.RUNNING.value},
            {'status': JobStatus.ERROR.value, 'message': 'test'},
        ]

        # call hook in mock context (w/ no wait b/w job checks)
        with pytest.raises(AirflowException):
            self.hook.wait_for_job(
                materialization_id=JOB_ID,
                wait_time=0,
            )

        # assert pdt_build_status called twice: first RUNNING, then ERROR
        calls = [
            mock.call(materialization_id=JOB_ID),
            mock.call(materialization_id=JOB_ID),
        ]
        mock_pdt_build_status.assert_has_calls(calls)

    @mock.patch(HOOK_PATH.format("get_looker_sdk"))
    def test_check_pdt_build(self, mock_sdk):
        # call hook in mock context
        self.hook.check_pdt_build(materialization_id=JOB_ID)

        # assert sdk constructor called once
        mock_sdk.assert_called_once_with()

        # assert sdk.check_pdt_build called once
        mock_sdk.return_value.check_pdt_build.assert_called_once_with(materialization_id=JOB_ID)

    @mock.patch(HOOK_PATH.format("get_looker_sdk"))
    def test_start_pdt_build(self, mock_sdk):
        # replace looker version invocation with mock response
        mock_sdk.return_value.versions.return_value.looker_release_version = "22.2.0"

        # call hook in mock context
        self.hook.start_pdt_build(
            model=MODEL,
            view=VIEW,
        )

        # assert sdk constructor called once
        mock_sdk.assert_called_once_with()

        # assert sdk.start_pdt_build called once
        mock_sdk.return_value.start_pdt_build.assert_called_once_with(
            model_name=MODEL,
            view_name=VIEW,
            source=SOURCE,
        )

    @mock.patch(HOOK_PATH.format("get_looker_sdk"))
    def test_stop_pdt_build(self, mock_sdk):
        # call hook in mock context
        self.hook.stop_pdt_build(materialization_id=JOB_ID)

        # assert sdk constructor called once
        mock_sdk.assert_called_once_with()

        # assert sdk.stop_pdt_build called once
        mock_sdk.return_value.stop_pdt_build.assert_called_once_with(
            materialization_id=JOB_ID,
            source=SOURCE,
        )
