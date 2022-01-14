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
#
import unittest
from unittest import mock

import pytest
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.looker import LookerHook, JobStatus

HOOK_PATH = "airflow_fork.airflow.providers.google.cloud.hooks.looker.LookerHook.{}"

JOB_ID = "test-id"
TASK_ID = "test-task-id"
MODEL = "test_model"
VIEW = "test_view"


# ALTERNATIVE: init hook with EXTRA
# CONN_EXTRA = {"config_file": "airflow_fork/looker.ini", "section": "Looker"}

class TestLookerHook(unittest.TestCase):
    def setUp(self):
        self.hook = LookerHook(looker_conn_id="test")

        # ALTERNATIVE: init with EXTRA
        # with mock.patch("airflow.hooks.base.BaseHook.get_connection") as conn:
        #     conn.return_value.extra_dejson = CONN_EXTRA
        #     self.hook = LookerHook(looker_conn_id="looker_default")

    @mock.patch(HOOK_PATH.format("pdt_build_status"))  # mock pdt_build_status method
    def test_wait_for_job(self, mock_pdt_build_status):
        # replace pdt_build_status invocation with mock status
        mock_pdt_build_status.side_effect = [
            JobStatus.RUNNING.value,
            JobStatus.ERROR.value,
        ]

        # call hook in mock context
        with pytest.raises(AirflowException):
            self.hook.wait_for_job(
                materialization_id=JOB_ID,
                wait_time=0,
            )

        # assert pdt_build_status called twice
        calls = [
            mock.call(materialization_id=JOB_ID),
            mock.call(materialization_id=JOB_ID),
        ]
        mock_pdt_build_status.has_calls(calls)

    @mock.patch(HOOK_PATH.format("get_looker_sdk"))  # mock get_looker_sdk method
    def test_check_pdt_build(self, mock_sdk):
        # call hook in mock context
        self.hook.check_pdt_build(materialization_id=JOB_ID)

        # assert sdk constructor called once
        mock_sdk.assert_called_once_with()

        # assert sdk's check_pdt_build called once
        mock_sdk.return_value.check_pdt_build.assert_called_once_with(materialization_id=JOB_ID)

    @mock.patch(HOOK_PATH.format("get_looker_sdk"))  # mock get_looker_sdk method
    def test_start_pdt_build(self, mock_sdk):
        # call hook in mock context
        self.hook.start_pdt_build(
            model=MODEL,
            view=VIEW,
        )

        # assert sdk constructor called once
        mock_sdk.assert_called_once_with()

        # assert sdk's start_pdt_build called once
        mock_sdk.return_value.start_pdt_build.assert_called_once_with(
            model=MODEL,
            view=VIEW,
        )

    @mock.patch(HOOK_PATH.format("get_looker_sdk"))  # mock get_looker_sdk method
    def test_stop_pdt_build(self, mock_sdk):
        # call hook in mock context
        self.hook.stop_pdt_build(materialization_id=JOB_ID)

        # assert sdk constructor called once
        mock_sdk.assert_called_once_with()

        # assert sdk's stop_pdt_build called once
        mock_sdk.return_value.stop_pdt_build.assert_called_once_with(materialization_id=JOB_ID)
