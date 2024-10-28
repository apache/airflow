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

from airflow.exceptions import AirflowException
from airflow.providers.teradata.operators.teradata_compute_cluster import (
    TeradataComputeClusterDecommissionOperator,
    TeradataComputeClusterProvisionOperator,
    TeradataComputeClusterResumeOperator,
    TeradataComputeClusterSuspendOperator,
    _single_result_row_handler,
)
from airflow.providers.teradata.triggers.teradata_compute_cluster import (
    TeradataComputeClusterSyncTrigger,
)
from airflow.providers.teradata.utils.constants import Constants


@pytest.fixture
def compute_profile_name():
    return "test_profile"


@pytest.fixture
def compute_group_name():
    return "test_group"


@pytest.fixture
def query_strategy():
    return "test_query_strategy"


@pytest.fixture
def compute_map():
    return "test_compute_map"


@pytest.fixture
def compute_attribute():
    return "test_compute_attribute"


@pytest.fixture
def compute_cluster_provision_instance(compute_profile_name):
    return TeradataComputeClusterProvisionOperator(
        task_id="test",
        compute_profile_name=compute_profile_name,
        teradata_conn_id="test_conn",
    )


@pytest.fixture
def compute_cluster_decommission_instance(compute_profile_name):
    return TeradataComputeClusterDecommissionOperator(
        task_id="test",
        compute_profile_name=compute_profile_name,
        teradata_conn_id="test_conn",
    )


@pytest.fixture
def compute_cluster_resume_instance(compute_profile_name):
    return TeradataComputeClusterResumeOperator(
        task_id="test",
        compute_profile_name=compute_profile_name,
        teradata_conn_id="test_conn",
    )


@pytest.fixture
def compute_cluster_suspend_instance(compute_profile_name):
    return TeradataComputeClusterSuspendOperator(
        task_id="test",
        compute_profile_name=compute_profile_name,
        teradata_conn_id="test_conn",
    )


class TestTeradataComputeClusterOperator:
    def test_compute_cluster_execute_invalid_profile(
        self, compute_cluster_provision_instance
    ):
        compute_cluster_provision_instance.compute_profile_name = None
        with pytest.raises(AirflowException):
            compute_cluster_provision_instance._compute_cluster_execute()

    def test_compute_cluster_execute_empty_profile(
        self, compute_cluster_provision_instance
    ):
        compute_cluster_provision_instance.compute_profile_name = ""
        with pytest.raises(AirflowException):
            compute_cluster_provision_instance._compute_cluster_execute()

    def test_compute_cluster_execute_none_profile(
        self, compute_cluster_provision_instance
    ):
        compute_cluster_provision_instance.compute_profile_name = "None"
        with pytest.raises(AirflowException):
            compute_cluster_provision_instance._compute_cluster_execute()

    def test_compute_cluster_execute_not_lake(self, compute_cluster_provision_instance):
        with patch.object(compute_cluster_provision_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = [None]
            with pytest.raises(AirflowException):
                compute_cluster_provision_instance._compute_cluster_execute()

    def test_compute_cluster_execute_not_lake_version_check(
        self, compute_cluster_provision_instance
    ):
        with patch.object(compute_cluster_provision_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "19"]
            with pytest.raises(AirflowException):
                compute_cluster_provision_instance._compute_cluster_execute()

    def test_compute_cluster_execute_not_lake_version_none(
        self, compute_cluster_provision_instance
    ):
        with patch.object(compute_cluster_provision_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", None]
            with pytest.raises(AirflowException):
                compute_cluster_provision_instance._compute_cluster_execute()

    def test_compute_cluster_execute_not_lake_version_invalid(
        self, compute_cluster_provision_instance
    ):
        with patch.object(compute_cluster_provision_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "invalid"]
            with pytest.raises(AirflowException):
                compute_cluster_provision_instance._compute_cluster_execute()

    def test_compute_cluster_execute_complete_success(
        self, compute_cluster_provision_instance
    ):
        event = {"status": "success", "message": "Success message"}
        # Call the method under test
        result = compute_cluster_provision_instance._compute_cluster_execute_complete(
            event
        )
        assert result == "Success message"

    def test_compute_cluster_execute_complete_error(
        self, compute_cluster_provision_instance
    ):
        event = {"status": "error", "message": "Error message"}
        with pytest.raises(AirflowException):
            compute_cluster_provision_instance._compute_cluster_execute_complete(event)

    def test_cc_execute_provision_new_cp(self, compute_cluster_provision_instance):
        with patch.object(compute_cluster_provision_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", None, "Success"]
            compute_profile_name = compute_cluster_provision_instance.compute_profile_name
            with patch.object(compute_cluster_provision_instance, "defer") as mock_defer:
                # Assert that defer method is called with the correct parameters
                expected_trigger = TeradataComputeClusterSyncTrigger(
                    teradata_conn_id=compute_cluster_provision_instance.teradata_conn_id,
                    compute_profile_name=compute_profile_name,
                    operation_type=Constants.CC_CREATE_OPR,
                    poll_interval=Constants.CC_POLL_INTERVAL,
                )
                mock_defer.return_value = expected_trigger
                result = compute_cluster_provision_instance._compute_cluster_execute()
                assert result == "Success"
                mock_defer.assert_called_once()
                mock_hook.run.assert_has_calls(
                    [
                        call(
                            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"CREATE COMPUTE PROFILE {compute_profile_name}",
                            handler=_single_result_row_handler,
                        ),
                    ]
                )

    def test_cc_execute_provision_exists_cp(self, compute_cluster_provision_instance):
        with patch.object(compute_cluster_provision_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", "RUNNING", "Success"]
            compute_profile_name = compute_cluster_provision_instance.compute_profile_name
            with patch.object(compute_cluster_provision_instance, "defer") as mock_defer:
                # Assert that defer method is called with the correct parameters
                expected_trigger = TeradataComputeClusterSyncTrigger(
                    teradata_conn_id=compute_cluster_provision_instance.teradata_conn_id,
                    compute_profile_name=compute_profile_name,
                    operation_type=Constants.CC_CREATE_OPR,
                    poll_interval=Constants.CC_POLL_INTERVAL,
                )
                mock_defer.return_value = expected_trigger
                result = compute_cluster_provision_instance._compute_cluster_execute()
                assert result == "RUNNING"
                mock_hook.run.assert_has_calls(
                    [
                        call(
                            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}')",
                            handler=_single_result_row_handler,
                        ),
                    ]
                )

    def test_cc_execute_provision_new_cp_exists_cg(
        self, compute_group_name, compute_cluster_provision_instance
    ):
        with patch.object(compute_cluster_provision_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", "1", None, "Success"]
            compute_cluster_provision_instance.compute_group_name = compute_group_name
            compute_profile_name = compute_cluster_provision_instance.compute_profile_name
            with patch.object(compute_cluster_provision_instance, "defer") as mock_defer:
                # Assert that defer method is called with the correct parameters
                expected_trigger = TeradataComputeClusterSyncTrigger(
                    teradata_conn_id=compute_cluster_provision_instance.teradata_conn_id,
                    compute_profile_name=compute_profile_name,
                    compute_group_name=compute_group_name,
                    operation_type=Constants.CC_CREATE_OPR,
                    poll_interval=Constants.CC_POLL_INTERVAL,
                )
                mock_defer.return_value = expected_trigger
                result = compute_cluster_provision_instance._compute_cluster_execute()
                assert result == "Success"
                mock_defer.assert_called_once()
                mock_hook.run.assert_has_calls(
                    [
                        call(
                            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SELECT  count(1) FROM DBC.ComputeGroups WHERE UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}') AND UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"CREATE COMPUTE PROFILE {compute_profile_name} IN {compute_group_name}",
                            handler=_single_result_row_handler,
                        ),
                    ]
                )

    def test_cc_execute_provision_exists_cp_exists_cg(
        self, compute_group_name, compute_cluster_provision_instance
    ):
        with patch.object(compute_cluster_provision_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", "1", "RUNNING", "Success"]
            compute_profile_name = compute_cluster_provision_instance.compute_profile_name
            compute_cluster_provision_instance.compute_group_name = compute_group_name
            with patch.object(compute_cluster_provision_instance, "defer") as mock_defer:
                # Assert that defer method is called with the correct parameters
                expected_trigger = TeradataComputeClusterSyncTrigger(
                    teradata_conn_id=compute_cluster_provision_instance.teradata_conn_id,
                    compute_profile_name=compute_profile_name,
                    compute_group_name=compute_group_name,
                    operation_type=Constants.CC_CREATE_OPR,
                    poll_interval=Constants.CC_POLL_INTERVAL,
                )
                mock_defer.return_value = expected_trigger
                result = compute_cluster_provision_instance._compute_cluster_execute()
                assert result == "RUNNING"
                mock_hook.run.assert_has_calls(
                    [
                        call(
                            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SELECT  count(1) FROM DBC.ComputeGroups WHERE UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}') AND UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                    ]
                )

    def test_cc_execute_provision_new_cp_new_cg(
        self, compute_group_name, compute_cluster_provision_instance
    ):
        with patch.object(compute_cluster_provision_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", "0", "Success", None, "Success"]
            compute_cluster_provision_instance.compute_group_name = compute_group_name
            compute_profile_name = compute_cluster_provision_instance.compute_profile_name
            with patch.object(compute_cluster_provision_instance, "defer") as mock_defer:
                # Assert that defer method is called with the correct parameters
                expected_trigger = TeradataComputeClusterSyncTrigger(
                    teradata_conn_id=compute_cluster_provision_instance.teradata_conn_id,
                    compute_profile_name=compute_profile_name,
                    compute_group_name=compute_group_name,
                    operation_type=Constants.CC_CREATE_OPR,
                    poll_interval=Constants.CC_POLL_INTERVAL,
                )
                mock_defer.return_value = expected_trigger
                result = compute_cluster_provision_instance._compute_cluster_execute()
                assert result == "Success"
                mock_defer.assert_called_once()
                mock_hook.run.assert_has_calls(
                    [
                        call(
                            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SELECT  count(1) FROM DBC.ComputeGroups WHERE UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"CREATE COMPUTE GROUP {compute_group_name}",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}') AND UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"CREATE COMPUTE PROFILE {compute_profile_name} IN {compute_group_name}",
                            handler=_single_result_row_handler,
                        ),
                    ]
                )

    def test_cc_execute_provision_new_cp_new_cg_with_options(
        self,
        compute_group_name,
        query_strategy,
        compute_map,
        compute_attribute,
        compute_cluster_provision_instance,
    ):
        with patch.object(compute_cluster_provision_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", "0", "Success", None, "Success"]
            compute_cluster_provision_instance.compute_group_name = compute_group_name
            compute_profile_name = compute_cluster_provision_instance.compute_profile_name
            compute_cluster_provision_instance.query_strategy = query_strategy
            compute_cluster_provision_instance.compute_map = compute_map
            compute_cluster_provision_instance.compute_attribute = compute_attribute

            with patch.object(compute_cluster_provision_instance, "defer") as mock_defer:
                # Assert that defer method is called with the correct parameters
                expected_trigger = TeradataComputeClusterSyncTrigger(
                    teradata_conn_id=compute_cluster_provision_instance.teradata_conn_id,
                    compute_profile_name=compute_profile_name,
                    compute_group_name=compute_group_name,
                    operation_type=Constants.CC_CREATE_OPR,
                    poll_interval=Constants.CC_POLL_INTERVAL,
                )
                mock_defer.return_value = expected_trigger
                result = compute_cluster_provision_instance._compute_cluster_execute()
                assert result == "Success"
                mock_defer.assert_called_once()
                mock_hook.run.assert_has_calls(
                    [
                        call(
                            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SELECT  count(1) FROM DBC.ComputeGroups WHERE UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"CREATE COMPUTE GROUP {compute_group_name} USING QUERY_STRATEGY ('{query_strategy}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}') AND UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"CREATE COMPUTE PROFILE {compute_profile_name} IN {compute_group_name}, "
                            f"INSTANCE = {compute_map}, INSTANCE TYPE = {query_strategy} USING {compute_attribute}",
                            handler=_single_result_row_handler,
                        ),
                    ]
                )

    def test_compute_cluster_execute_drop_cp(self, compute_cluster_decommission_instance):
        with patch.object(compute_cluster_decommission_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", None]
            compute_profile_name = (
                compute_cluster_decommission_instance.compute_profile_name
            )
            compute_cluster_decommission_instance._compute_cluster_execute()
            mock_hook.run.assert_has_calls(
                [
                    call(
                        "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                        handler=_single_result_row_handler,
                    ),
                    call(
                        "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                        handler=_single_result_row_handler,
                    ),
                    call(
                        f"DROP COMPUTE PROFILE {compute_profile_name}",
                        handler=_single_result_row_handler,
                    ),
                ]
            )

    def test_compute_cluster_execute_drop_cp_cg(
        self, compute_cluster_decommission_instance, compute_group_name
    ):
        with patch.object(compute_cluster_decommission_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", None, None]
            compute_profile_name = (
                compute_cluster_decommission_instance.compute_profile_name
            )
            compute_cluster_decommission_instance.compute_group_name = compute_group_name
            compute_cluster_decommission_instance.delete_compute_group = True
            compute_cluster_decommission_instance._compute_cluster_execute()
            mock_hook.run.assert_has_calls(
                [
                    call(
                        "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                        handler=_single_result_row_handler,
                    ),
                    call(
                        "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                        handler=_single_result_row_handler,
                    ),
                    call(
                        f"DROP COMPUTE PROFILE {compute_profile_name} IN COMPUTE GROUP {compute_group_name}",
                        handler=_single_result_row_handler,
                    ),
                    call(
                        f"DROP COMPUTE GROUP {compute_group_name}",
                        handler=_single_result_row_handler,
                    ),
                ]
            )

    def test_compute_cluster_execute_resume_success(
        self, compute_cluster_resume_instance
    ):
        with patch.object(compute_cluster_resume_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", "Suspended", "Success"]
            compute_profile_name = compute_cluster_resume_instance.compute_profile_name

            with patch.object(compute_cluster_resume_instance, "defer") as mock_defer:
                expected_trigger = TeradataComputeClusterSyncTrigger(
                    teradata_conn_id=compute_cluster_resume_instance.teradata_conn_id,
                    compute_profile_name=compute_profile_name,
                    operation_type=Constants.CC_RESUME_OPR,
                    poll_interval=Constants.CC_POLL_INTERVAL,
                )
                mock_defer.return_value = expected_trigger
                result = compute_cluster_resume_instance._compute_cluster_execute()
                assert result == "Success"
                mock_defer.assert_called_once()
                mock_hook.run.assert_has_calls(
                    [
                        call(
                            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"RESUME COMPUTE FOR COMPUTE PROFILE {compute_profile_name}",
                            handler=_single_result_row_handler,
                        ),
                    ]
                )

    def test_compute_cluster_execute_resume_cg_success(
        self, compute_group_name, compute_cluster_resume_instance
    ):
        with patch.object(compute_cluster_resume_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", "Suspended", "Success"]
            compute_profile_name = compute_cluster_resume_instance.compute_profile_name
            compute_cluster_resume_instance.compute_group_name = compute_group_name
            with patch.object(compute_cluster_resume_instance, "defer") as mock_defer:
                expected_trigger = TeradataComputeClusterSyncTrigger(
                    teradata_conn_id=compute_cluster_resume_instance.teradata_conn_id,
                    compute_profile_name=compute_profile_name,
                    operation_type=Constants.CC_RESUME_OPR,
                    poll_interval=Constants.CC_POLL_INTERVAL,
                )
                mock_defer.return_value = expected_trigger
                result = compute_cluster_resume_instance._compute_cluster_execute()
                assert result == "Success"
                mock_defer.assert_called_once()
                mock_hook.run.assert_has_calls(
                    [
                        call(
                            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}')"
                            f" AND UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"RESUME COMPUTE FOR COMPUTE PROFILE {compute_profile_name} IN COMPUTE GROUP {compute_group_name}",
                            handler=_single_result_row_handler,
                        ),
                    ]
                )

    def test_compute_cluster_execute_resume_cc_not_exists(
        self, compute_group_name, compute_cluster_resume_instance
    ):
        with patch.object(compute_cluster_resume_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", None]
            compute_profile_name = compute_cluster_resume_instance.compute_profile_name
            with pytest.raises(AirflowException):
                compute_cluster_resume_instance._compute_cluster_execute()
            mock_hook.run.assert_has_calls(
                [
                    call(
                        "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                        handler=_single_result_row_handler,
                    ),
                    call(
                        "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                        handler=_single_result_row_handler,
                    ),
                    call(
                        f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}')",
                        handler=_single_result_row_handler,
                    ),
                ]
            )

    def test_compute_cluster_execute_resume_same_state(
        self, compute_cluster_resume_instance
    ):
        with patch.object(compute_cluster_resume_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", "Running"]
            compute_profile_name = compute_cluster_resume_instance.compute_profile_name
            result = compute_cluster_resume_instance._compute_cluster_execute()
            assert result is None
            mock_hook.run.assert_has_calls(
                [
                    call(
                        "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                        handler=_single_result_row_handler,
                    ),
                    call(
                        "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                        handler=_single_result_row_handler,
                    ),
                    call(
                        f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}')",
                        handler=_single_result_row_handler,
                    ),
                ]
            )

    def test_compute_cluster_execute_suspend_success(
        self, compute_cluster_suspend_instance
    ):
        with patch.object(compute_cluster_suspend_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", "Running", "Success"]
            compute_profile_name = compute_cluster_suspend_instance.compute_profile_name

            with patch.object(compute_cluster_suspend_instance, "defer") as mock_defer:
                expected_trigger = TeradataComputeClusterSyncTrigger(
                    teradata_conn_id=compute_cluster_suspend_instance.teradata_conn_id,
                    compute_profile_name=compute_profile_name,
                    operation_type=Constants.CC_RESUME_OPR,
                    poll_interval=Constants.CC_POLL_INTERVAL,
                )
                mock_defer.return_value = expected_trigger
                result = compute_cluster_suspend_instance._compute_cluster_execute()
                assert result == "Success"
                mock_defer.assert_called_once()
                mock_hook.run.assert_has_calls(
                    [
                        call(
                            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SUSPEND COMPUTE FOR COMPUTE PROFILE {compute_profile_name}",
                            handler=_single_result_row_handler,
                        ),
                    ]
                )

    def test_compute_cluster_execute_suspend_cg_success(
        self, compute_group_name, compute_cluster_suspend_instance
    ):
        with patch.object(compute_cluster_suspend_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", "Running", "Success"]
            compute_profile_name = compute_cluster_suspend_instance.compute_profile_name
            compute_cluster_suspend_instance.compute_group_name = compute_group_name
            with patch.object(compute_cluster_suspend_instance, "defer") as mock_defer:
                expected_trigger = TeradataComputeClusterSyncTrigger(
                    teradata_conn_id=compute_cluster_suspend_instance.teradata_conn_id,
                    compute_profile_name=compute_profile_name,
                    operation_type=Constants.CC_RESUME_OPR,
                    poll_interval=Constants.CC_POLL_INTERVAL,
                )
                mock_defer.return_value = expected_trigger
                result = compute_cluster_suspend_instance._compute_cluster_execute()
                assert result == "Success"
                mock_defer.assert_called_once()
                mock_hook.run.assert_has_calls(
                    [
                        call(
                            "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}')"
                            f" AND UPPER(ComputeGroupName) = UPPER('{compute_group_name}')",
                            handler=_single_result_row_handler,
                        ),
                        call(
                            f"SUSPEND COMPUTE FOR COMPUTE PROFILE {compute_profile_name} IN COMPUTE GROUP {compute_group_name}",
                            handler=_single_result_row_handler,
                        ),
                    ]
                )

    def test_compute_cluster_execute_suspend_cc_not_exists(
        self, compute_group_name, compute_cluster_suspend_instance
    ):
        with patch.object(compute_cluster_suspend_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", None]
            compute_profile_name = compute_cluster_suspend_instance.compute_profile_name
            with pytest.raises(AirflowException):
                compute_cluster_suspend_instance._compute_cluster_execute()
            mock_hook.run.assert_has_calls(
                [
                    call(
                        "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                        handler=_single_result_row_handler,
                    ),
                    call(
                        "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                        handler=_single_result_row_handler,
                    ),
                    call(
                        f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}')",
                        handler=_single_result_row_handler,
                    ),
                ]
            )

    def test_compute_cluster_execute_suspend_same_state(
        self, compute_cluster_suspend_instance
    ):
        with patch.object(compute_cluster_suspend_instance, "hook") as mock_hook:
            # Set up mock return values
            mock_hook.run.side_effect = ["1", "20.00", "Suspended"]
            compute_profile_name = compute_cluster_suspend_instance.compute_profile_name
            result = compute_cluster_suspend_instance._compute_cluster_execute()
            assert result is None
            mock_hook.run.assert_has_calls(
                [
                    call(
                        "SELECT count(1) from DBC.StorageV WHERE StorageName='TD_OFSSTORAGE'",
                        handler=_single_result_row_handler,
                    ),
                    call(
                        "SELECT  InfoData AS Version FROM DBC.DBCInfoV WHERE InfoKey = 'VERSION'",
                        handler=_single_result_row_handler,
                    ),
                    call(
                        f"SEL ComputeProfileState FROM DBC.ComputeProfilesVX WHERE UPPER(ComputeProfileName) = UPPER('{compute_profile_name}')",
                        handler=_single_result_row_handler,
                    ),
                ]
            )
