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

from unittest.mock import call, patch

import pytest

from airflow.providers.common.sql.hooks.sql import fetch_one_handler
from airflow.providers.teradata.hooks.teradata import TeradataHook
from airflow.providers.teradata.triggers.teradata_compute_cluster import TeradataComputeClusterSyncTrigger
from airflow.providers.teradata.utils.constants import Constants
from airflow.triggers.base import TriggerEvent


@pytest.mark.asyncio
async def test_run_suspend_success():
    trigger = TeradataComputeClusterSyncTrigger(
        teradata_conn_id="test_conn_id",
        compute_profile_name="test_profile",
        operation_type=Constants.CC_SUSPEND_OPR,
        poll_interval=1,
    )
    with patch.object(trigger, "get_status") as mock_get_status:
        mock_get_status.return_value = Constants.CC_SUSPEND_DB_STATUS
        async for event in trigger.run():
            assert event == TriggerEvent(
                {
                    "status": "success",
                    "message": Constants.CC_OPR_SUCCESS_STATUS_MSG
                    % ("test_profile", Constants.CC_SUSPEND_OPR),
                }
            )
        mock_get_status.assert_called_once()


@pytest.mark.asyncio
async def test_run_suspend_success_cg():
    trigger = TeradataComputeClusterSyncTrigger(
        teradata_conn_id="test_conn_id",
        compute_profile_name="test_profile",
        operation_type=Constants.CC_SUSPEND_OPR,
        poll_interval=1,
    )
    with patch.object(trigger, "get_status") as mock_get_status:
        mock_get_status.return_value = Constants.CC_SUSPEND_DB_STATUS
        async for event in trigger.run():
            assert event == TriggerEvent(
                {
                    "status": "success",
                    "message": Constants.CC_OPR_SUCCESS_STATUS_MSG
                    % ("test_profile", Constants.CC_SUSPEND_OPR),
                }
            )
        mock_get_status.assert_called_once()


@pytest.mark.asyncio
async def test_run_suspend_failure():
    trigger = TeradataComputeClusterSyncTrigger(
        teradata_conn_id="test_conn_id",
        compute_profile_name="test_profile",
        operation_type=Constants.CC_SUSPEND_OPR,
        poll_interval=1,
    )
    with patch.object(trigger, "get_status") as mock_get_status:
        mock_get_status.return_value = None
        async for event in trigger.run():
            assert event == TriggerEvent({"status": "error", "message": Constants.CC_GRP_PRP_NON_EXISTS_MSG})
        mock_get_status.assert_called_once()


@pytest.mark.asyncio
async def test_run_resume_success():
    trigger = TeradataComputeClusterSyncTrigger(
        teradata_conn_id="test_conn_id",
        compute_profile_name="test_profile",
        operation_type=Constants.CC_RESUME_OPR,
        poll_interval=1,
    )
    with patch.object(trigger, "get_status") as mock_get_status:
        mock_get_status.return_value = Constants.CC_RESUME_DB_STATUS
        async for event in trigger.run():
            assert event == TriggerEvent(
                {
                    "status": "success",
                    "message": Constants.CC_OPR_SUCCESS_STATUS_MSG
                    % ("test_profile", Constants.CC_RESUME_OPR),
                }
            )
        mock_get_status.assert_called_once()


@pytest.mark.asyncio
async def test_run_resume_failure():
    trigger = TeradataComputeClusterSyncTrigger(
        teradata_conn_id="test_conn_id",
        compute_profile_name="test_profile",
        operation_type=Constants.CC_RESUME_OPR,
        poll_interval=1,
    )
    with patch.object(trigger, "get_status") as mock_get_status:
        mock_get_status.return_value = None
        async for event in trigger.run():
            assert event == TriggerEvent({"status": "error", "message": Constants.CC_GRP_PRP_NON_EXISTS_MSG})
        mock_get_status.assert_called_once()


@pytest.fixture
def mock_teradata_hook_run():
    with patch.object(TeradataHook, "run") as mock_run:
        yield mock_run


@pytest.mark.asyncio
async def test_get_status(mock_teradata_hook_run):
    trigger = TeradataComputeClusterSyncTrigger(
        teradata_conn_id="test_conn_id",
        compute_profile_name="test_profile",
        operation_type=Constants.CC_SUSPEND_OPR,
        poll_interval=1,
    )
    mock_teradata_hook_run.return_value = [Constants.CC_SUSPEND_DB_STATUS]
    status = await trigger.get_status()
    assert status == Constants.CC_SUSPEND_DB_STATUS
    mock_teradata_hook_run.assert_called_once()
    mock_teradata_hook_run.assert_has_calls(
        [
            call(
                "SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = "
                "UPPER('test_profile')",
                handler=fetch_one_handler,
            ),
        ]
    )


@pytest.mark.asyncio
async def test_get_status_cg(mock_teradata_hook_run):
    trigger = TeradataComputeClusterSyncTrigger(
        teradata_conn_id="test_conn_id",
        compute_profile_name="test_profile",
        compute_group_name="test_group",
        operation_type=Constants.CC_RESUME_OPR,
        poll_interval=1,
    )
    mock_teradata_hook_run.return_value = [Constants.CC_RESUME_DB_STATUS]
    status = await trigger.get_status()
    assert status == Constants.CC_RESUME_DB_STATUS
    mock_teradata_hook_run.assert_called_once()
    mock_teradata_hook_run.assert_has_calls(
        [
            call(
                "SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = "
                "UPPER('test_profile') AND UPPER(ComputeGroupName) = UPPER('test_group')",
                handler=fetch_one_handler,
            ),
        ]
    )
