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

from unittest import mock

import boto3
import botocore
import pytest

from airflow.providers.amazon.aws.hooks.neptune_analytics import NeptuneAnalyticsHook


class TestNeptuneAnalyticsCustomWaiters:
    def test_service_waiters(self):
        """The custom waiter file must be discovered and loaded by the hook.

        Regression test: the waiter file must be named to match the hook's
        ``client_type`` (``neptune-graph``) so that ``AwsGenericHook.waiter_path``
        resolves it. If the file name does not match, ``list_waiters`` silently
        falls back to botocore's official waiters and the custom
        ``import_task_cancelled`` acceptors become dead code.
        """
        assert "import_task_cancelled" in NeptuneAnalyticsHook().list_waiters()

    def test_custom_import_task_cancelled_waiter_is_used(self):
        """The custom (not botocore-official) waiter definition must be selected.

        The custom waiter treats a fast-completing import that reaches SUCCEEDED
        (or FAILED) before the cancel takes effect as *success*, whereas
        botocore's official ImportTaskCancelled waiter treats anything other than
        CANCELLING/CANCELLED as failure. This asserts the custom acceptors win.
        """
        hook = NeptuneAnalyticsHook()
        assert hook.waiter_path is not None
        assert "import_task_cancelled" in hook._list_custom_waiters()


class TestNeptuneAnalyticsImportTaskCancelledWaiter:
    WAITER_NAME = "import_task_cancelled"
    TASK_ID = "t-abc123"

    @pytest.fixture(autouse=True)
    def mock_conn(self, monkeypatch):
        self.client = boto3.client("neptune-graph", region_name="us-east-1")
        monkeypatch.setattr(NeptuneAnalyticsHook, "conn", self.client)

    @pytest.fixture
    def mock_get_import_task(self):
        with mock.patch.object(self.client, "get_import_task") as mock_getter:
            yield mock_getter

    @pytest.mark.parametrize("state", ["CANCELLED", "SUCCEEDED", "FAILED"])
    def test_import_task_cancelled_success_states(self, state, mock_get_import_task):
        # SUCCEEDED / FAILED can happen when the import finishes before the cancel
        # takes effect. The custom waiter must treat these as success, not failure.
        mock_get_import_task.return_value = {"status": state}

        NeptuneAnalyticsHook().get_waiter(self.WAITER_NAME).wait(
            taskIdentifier=self.TASK_ID,
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
        )

    def test_import_task_cancelled_failure_state(self, mock_get_import_task):
        mock_get_import_task.return_value = {"status": "ERROR_ENCOUNTERED"}
        with pytest.raises(botocore.exceptions.WaiterError):
            NeptuneAnalyticsHook().get_waiter(self.WAITER_NAME).wait(
                taskIdentifier=self.TASK_ID,
                WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
            )

    def test_import_task_cancelled_wait(self, mock_get_import_task):
        cancelling = {"status": "CANCELLING"}
        cancelled = {"status": "CANCELLED"}
        mock_get_import_task.side_effect = [cancelling, cancelling, cancelled]

        NeptuneAnalyticsHook().get_waiter(self.WAITER_NAME).wait(
            taskIdentifier=self.TASK_ID,
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
        )
