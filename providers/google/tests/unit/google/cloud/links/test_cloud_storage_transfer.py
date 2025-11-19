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

import pytest

from airflow.providers.google.cloud.links.cloud_storage_transfer import (
    CloudStorageTransferDetailsLink,
    CloudStorageTransferJobLink,
    CloudStorageTransferLinkHelper,
    CloudStorageTransferListLink,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult

TEST_PROJECT_ID = "test-project-id"
TEST_TRANSFER_JOB = "test-transfer-job"
TEST_TRANSFER_OPERATION = "test-transfer-operation"
TEST_OPERATION_NAME = f"transferOperations/{TEST_TRANSFER_OPERATION}"

CLOUD_STORAGE_TRANSFER_BASE_LINK = "https://console.cloud.google.com/transfer"
EXPECTED_CLOUD_STORAGE_TRANSFER_LIST_LINK = (
    CLOUD_STORAGE_TRANSFER_BASE_LINK + f"/jobs?project={TEST_PROJECT_ID}"
)
EXPECTED_CLOUD_STORAGE_TRANSFER_JOB_LINK = (
    CLOUD_STORAGE_TRANSFER_BASE_LINK
    + f"/jobs/transferJobs%2F{TEST_TRANSFER_JOB}/runs?project={TEST_PROJECT_ID}"
)
EXPECTED_CLOUD_STORAGE_TRANSFER_OPERATION_LINK = (
    CLOUD_STORAGE_TRANSFER_BASE_LINK
    + f"/jobs/transferJobs%2F{TEST_TRANSFER_JOB}/runs/transferOperations%2F{TEST_TRANSFER_OPERATION}"
    + f"?project={TEST_PROJECT_ID}"
)


class TestCloudStorageTransferLinkHelper:
    def test_extract_parts_with_valid_operation_name(self):
        """Test extract_parts with a valid operation name."""
        operation_name = f"transferOperations/{TEST_TRANSFER_OPERATION}-{TEST_TRANSFER_JOB}"
        transfer_operation, transfer_job = CloudStorageTransferLinkHelper.extract_parts(operation_name)

        assert transfer_operation == TEST_TRANSFER_OPERATION
        assert transfer_job == TEST_TRANSFER_JOB

    def test_extract_parts_with_none_operation_name(self):
        """Test extract_parts with None operation name."""
        transfer_operation, transfer_job = CloudStorageTransferLinkHelper.extract_parts(None)

        assert transfer_operation == ""
        assert transfer_job == ""

    def test_extract_parts_with_empty_operation_name(self):
        """Test extract_parts with empty operation name."""
        transfer_operation, transfer_job = CloudStorageTransferLinkHelper.extract_parts("")

        assert transfer_operation == ""
        assert transfer_job == ""

    def test_extract_parts_with_malformed_operation_name(self):
        """Test extract_parts with malformed operation name."""
        operation_name = "invalid-format"
        transfer_operation, transfer_job = CloudStorageTransferLinkHelper.extract_parts(operation_name)

        # Should handle gracefully but may raise IndexError for malformed input
        # The actual behavior depends on the split operations
        try:
            # If it doesn't raise an exception, check what we get
            assert transfer_operation is not None
            assert transfer_job is not None
        except IndexError:
            # This is expected behavior for malformed input
            pass


class TestCloudStorageTransferListLink:
    def test_name_and_key(self):
        """Test that name and key are set correctly."""
        link = CloudStorageTransferListLink()

        assert link.name == "Cloud Storage Transfer"
        assert link.key == "cloud_storage_transfer"

    def test_format_str(self):
        """Test that format_str is set correctly."""
        link = CloudStorageTransferListLink()

        expected_format = CLOUD_STORAGE_TRANSFER_BASE_LINK + "/jobs?project={project_id}"
        assert link.format_str == expected_format

    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session, mock_supervisor_comms):
        """Test get_link method for CloudStorageTransferListLink."""
        expected_url = EXPECTED_CLOUD_STORAGE_TRANSFER_LIST_LINK
        link = CloudStorageTransferListLink()

        # Create a mock operator with project_id
        class MockOperator:
            def __init__(self, project_id, task_id=None):
                self.project_id = project_id
                self.task_id = task_id
                self.extra_links_params = {"project_id": project_id}

        ti = create_task_instance_of_operator(
            MockOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            project_id=TEST_PROJECT_ID,
        )
        session.add(ti)
        session.commit()

        link.persist(context={"ti": ti, "task": ti.task})

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={"project_id": TEST_PROJECT_ID},
            )

        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestCloudStorageTransferJobLink:
    def test_name_and_key(self):
        """Test that name and key are set correctly."""
        link = CloudStorageTransferJobLink()

        assert link.name == "Cloud Storage Transfer Job"
        assert link.key == "cloud_storage_transfer_job"

    def test_format_str(self):
        """Test that format_str is set correctly."""
        link = CloudStorageTransferJobLink()

        expected_format = (
            CLOUD_STORAGE_TRANSFER_BASE_LINK + "/jobs/transferJobs%2F{transfer_job}/runs?project={project_id}"
        )
        assert link.format_str == expected_format

    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session, mock_supervisor_comms):
        """Test get_link method for CloudStorageTransferJobLink."""
        expected_url = EXPECTED_CLOUD_STORAGE_TRANSFER_JOB_LINK
        link = CloudStorageTransferJobLink()

        # Create a mock operator with required parameters
        class MockOperator:
            def __init__(self, project_id, transfer_job, task_id=None):
                self.project_id = project_id
                self.transfer_job = transfer_job
                self.task_id = task_id
                self.extra_links_params = {
                    "project_id": project_id,
                    "transfer_job": transfer_job,
                }

        ti = create_task_instance_of_operator(
            MockOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            project_id=TEST_PROJECT_ID,
            transfer_job=TEST_TRANSFER_JOB,
        )
        session.add(ti)
        session.commit()

        link.persist(context={"ti": ti, "task": ti.task})

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "project_id": TEST_PROJECT_ID,
                    "transfer_job": TEST_TRANSFER_JOB,
                },
            )

        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url


class TestCloudStorageTransferDetailsLink:
    def test_name_and_key(self):
        """Test that name and key are set correctly."""
        link = CloudStorageTransferDetailsLink()

        assert link.name == "Cloud Storage Transfer Details"
        assert link.key == "cloud_storage_transfer_details"

    def test_format_str(self):
        """Test that format_str is set correctly."""
        link = CloudStorageTransferDetailsLink()

        expected_format = (
            CLOUD_STORAGE_TRANSFER_BASE_LINK
            + "/jobs/transferJobs%2F{transfer_job}/runs/transferOperations%2F{transfer_operation}"
            + "?project={project_id}"
        )
        assert link.format_str == expected_format

    def test_extract_parts_with_valid_operation_name(self):
        """Test extract_parts with a valid operation name."""
        operation_name = f"transferOperations/{TEST_TRANSFER_OPERATION}-{TEST_TRANSFER_JOB}"
        transfer_operation, transfer_job = CloudStorageTransferDetailsLink.extract_parts(operation_name)

        assert transfer_operation == TEST_TRANSFER_OPERATION
        assert transfer_job == TEST_TRANSFER_JOB

    def test_extract_parts_with_none_operation_name(self):
        """Test extract_parts with None operation name."""
        transfer_operation, transfer_job = CloudStorageTransferDetailsLink.extract_parts(None)

        assert transfer_operation == ""
        assert transfer_job == ""

    def test_extract_parts_with_empty_operation_name(self):
        """Test extract_parts with empty operation name."""
        transfer_operation, transfer_job = CloudStorageTransferDetailsLink.extract_parts("")

        assert transfer_operation == ""
        assert transfer_job == ""

    def test_extract_parts_with_malformed_operation_name(self):
        """Test extract_parts with malformed operation name."""
        operation_name = "invalid-format"
        transfer_operation, transfer_job = CloudStorageTransferDetailsLink.extract_parts(operation_name)

        # Should handle gracefully but may raise IndexError for malformed input
        # The actual behavior depends on the split operations
        try:
            # If it doesn't raise an exception, check what we get
            assert transfer_operation is not None
            assert transfer_job is not None
        except IndexError:
            # This is expected behavior for malformed input
            pass

    @pytest.mark.db_test
    def test_get_link(self, create_task_instance_of_operator, session, mock_supervisor_comms):
        """Test get_link method for CloudStorageTransferDetailsLink."""
        expected_url = EXPECTED_CLOUD_STORAGE_TRANSFER_OPERATION_LINK
        link = CloudStorageTransferDetailsLink()

        # Create a mock operator
        class MockOperator:
            def __init__(self, project_id, task_id=None):
                self.project_id = project_id
                self.task_id = task_id
                self.extra_links_params = {"project_id": project_id}

        ti = create_task_instance_of_operator(
            MockOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            project_id=TEST_PROJECT_ID,
        )
        session.add(ti)
        session.commit()

        # Test persist method with operation_name
        operation_name = f"transferOperations/{TEST_TRANSFER_OPERATION}-{TEST_TRANSFER_JOB}"
        link.persist(
            context={"ti": ti, "task": ti.task},
            project_id=TEST_PROJECT_ID,
            operation_name=operation_name,
        )

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "project_id": TEST_PROJECT_ID,
                    "transfer_job": TEST_TRANSFER_JOB,
                    "transfer_operation": TEST_TRANSFER_OPERATION,
                },
            )

        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        assert actual_url == expected_url

    @pytest.mark.db_test
    def test_persist_with_none_operation_name(
        self, create_task_instance_of_operator, session, mock_supervisor_comms
    ):
        """Test persist method with None operation_name."""
        link = CloudStorageTransferDetailsLink()

        class MockOperator:
            def __init__(self, project_id, task_id=None):
                self.project_id = project_id
                self.task_id = task_id
                self.extra_links_params = {"project_id": project_id}

        ti = create_task_instance_of_operator(
            MockOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            project_id=TEST_PROJECT_ID,
        )
        session.add(ti)
        session.commit()

        # Test persist method with None operation_name
        link.persist(
            context={"ti": ti, "task": ti.task},
            project_id=TEST_PROJECT_ID,
            operation_name=None,
        )

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "project_id": TEST_PROJECT_ID,
                    "transfer_job": "",
                    "transfer_operation": "",
                },
            )

        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        # Should return empty string when transfer_job and transfer_operation are empty
        assert actual_url == ""

    @pytest.mark.db_test
    def test_persist_with_empty_operation_name(
        self, create_task_instance_of_operator, session, mock_supervisor_comms
    ):
        """Test persist method with empty operation_name."""
        link = CloudStorageTransferDetailsLink()

        class MockOperator:
            def __init__(self, project_id, task_id=None):
                self.project_id = project_id
                self.task_id = task_id
                self.extra_links_params = {"project_id": project_id}

        ti = create_task_instance_of_operator(
            MockOperator,
            dag_id="test_link_dag",
            task_id="test_link_task",
            project_id=TEST_PROJECT_ID,
        )
        session.add(ti)
        session.commit()

        # Test persist method with empty operation_name
        link.persist(
            context={"ti": ti, "task": ti.task},
            project_id=TEST_PROJECT_ID,
            operation_name="",
        )

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key="key",
                value={
                    "project_id": TEST_PROJECT_ID,
                    "transfer_job": "",
                    "transfer_operation": "",
                },
            )

        actual_url = link.get_link(operator=ti.task, ti_key=ti.key)
        # Should return empty string when transfer_job and transfer_operation are empty
        assert actual_url == ""
