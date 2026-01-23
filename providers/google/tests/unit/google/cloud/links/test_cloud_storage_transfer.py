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

"""Real tests for Google Cloud Storage Transfer Service links."""

from __future__ import annotations

import pytest

from airflow.providers.google.cloud.links.cloud_storage_transfer import (
    CloudStorageTransferDetailsLink,
    CloudStorageTransferJobLink,
    CloudStorageTransferLinkHelper,
    CloudStorageTransferListLink,
)

REAL_PROJECT_ID = "my-gcp-project-123456"
REAL_TRANSFER_JOB = "transferJobs-1234567890123456789"
REAL_TRANSFER_OPERATION = "transferOperations/9876543210987654321"
REAL_OPERATION_NAME = f"{REAL_TRANSFER_OPERATION}-{REAL_TRANSFER_JOB}"

EXPECTED_LIST_URL = f"https://console.cloud.google.com/transfer/jobs?project={REAL_PROJECT_ID}"
EXPECTED_JOB_URL = f"https://console.cloud.google.com/transfer/jobs/transferJobs%2F{REAL_TRANSFER_JOB}/runs?project={REAL_PROJECT_ID}"
EXPECTED_DETAILS_URL = (
    f"https://console.cloud.google.com/transfer/jobs/transferJobs%2FtransferJobs"
    f"/runs/transferOperations%2F9876543210987654321-transferJobs-1234567890123456789?project={REAL_PROJECT_ID}"
)


class TestCloudStorageTransferLinkHelper:
    """Test the CloudStorageTransferLinkHelper with real operation names."""

    def test_extract_parts_with_real_operation_name(self):
        """Test extract_parts with a real Google Cloud Storage Transfer operation name."""
        transfer_operation, transfer_job = CloudStorageTransferLinkHelper.extract_parts(REAL_OPERATION_NAME)

        assert transfer_operation == "9876543210987654321-transferJobs-1234567890123456789"
        assert transfer_job == "transferJobs"

    def test_extract_parts_with_various_real_formats(self):
        """Test extract_parts with various real operation name formats."""
        test_cases = [
            ("transferOperations/12345-transferJobs-67890", "12345-transferJobs-67890", "transferJobs"),
            ("transferOperations/op123-transferJobs-job456", "op123-transferJobs-job456", "transferJobs"),
            (
                "transferOperations/99999999999999999999-transferJobs-11111111111111111111",
                "99999999999999999999-transferJobs-11111111111111111111",
                "transferJobs",
            ),
        ]

        for operation_name, expected_operation, expected_job in test_cases:
            transfer_operation, transfer_job = CloudStorageTransferLinkHelper.extract_parts(operation_name)
            assert transfer_operation == expected_operation
            assert transfer_job == expected_job

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

    def test_extract_parts_with_malformed_operation_names(self):
        """Test extract_parts with malformed operation names that might occur in real scenarios."""
        test_cases = [
            ("invalid-format", IndexError),
            ("transferOperations/", IndexError),
            ("transferOperations/123", IndexError),
            ("transferOperations/123-", ("123-", "")),
            ("-transferJobs-job456", IndexError),
            ("transferOperations/123-transferJobs", ("123-transferJobs", "transferJobs")),
        ]

        for operation_name, expected in test_cases:
            if expected is IndexError:
                with pytest.raises(IndexError):
                    CloudStorageTransferLinkHelper.extract_parts(operation_name)
            else:
                transfer_operation, transfer_job = CloudStorageTransferLinkHelper.extract_parts(
                    operation_name
                )
                assert transfer_operation == expected[0]
                assert transfer_job == expected[1]


class TestCloudStorageTransferListLink:
    """Test the CloudStorageTransferListLink with real scenarios."""

    def test_link_properties(self):
        """Test that link properties are set correctly for real usage."""
        link = CloudStorageTransferListLink()

        assert link.name == "Cloud Storage Transfer"
        assert link.key == "cloud_storage_transfer"
        assert "{project_id}" in link.format_str

    def test_format_str_with_real_project_id(self):
        """Test format_str generates correct URL for real project ID."""
        link = CloudStorageTransferListLink()
        formatted_url = link.format_str.format(project_id=REAL_PROJECT_ID)

        assert formatted_url == EXPECTED_LIST_URL
        assert formatted_url.startswith("https://console.cloud.google.com/transfer/jobs?project=")
        assert REAL_PROJECT_ID in formatted_url

    def test_format_str_with_various_project_ids(self):
        """Test format_str with various real project ID formats."""
        project_ids = [
            "my-project",
            "project-123456",
            "my-gcp-project-123",
            "a" * 30,
        ]

        link = CloudStorageTransferListLink()
        for project_id in project_ids:
            formatted_url = link.format_str.format(project_id=project_id)
            assert project_id in formatted_url
            assert formatted_url.startswith("https://console.cloud.google.com/transfer/jobs?")


class TestCloudStorageTransferJobLink:
    """Test the CloudStorageTransferJobLink with real scenarios."""

    def test_link_properties(self):
        """Test that link properties are set correctly for real usage."""
        link = CloudStorageTransferJobLink()

        assert link.name == "Cloud Storage Transfer Job"
        assert link.key == "cloud_storage_transfer_job"
        assert "{project_id}" in link.format_str
        assert "{transfer_job}" in link.format_str

    def test_format_str_with_real_parameters(self):
        """Test format_str generates correct URL for real transfer job."""
        link = CloudStorageTransferJobLink()
        formatted_url = link.format_str.format(project_id=REAL_PROJECT_ID, transfer_job=REAL_TRANSFER_JOB)

        assert formatted_url == EXPECTED_JOB_URL
        assert formatted_url.startswith("https://console.cloud.google.com/transfer/jobs/transferJobs%2F")
        assert REAL_PROJECT_ID in formatted_url
        assert REAL_TRANSFER_JOB in formatted_url

    def test_format_str_url_encoding(self):
        """Test that transfer job ID is properly URL encoded."""
        link = CloudStorageTransferJobLink()
        formatted_url = link.format_str.format(project_id=REAL_PROJECT_ID, transfer_job=REAL_TRANSFER_JOB)

        assert "transferJobs%2F" in formatted_url
        assert "transferJobs/" not in formatted_url


class TestCloudStorageTransferDetailsLink:
    """Test the CloudStorageTransferDetailsLink with real scenarios."""

    def test_link_properties(self):
        """Test that link properties are set correctly for real usage."""
        link = CloudStorageTransferDetailsLink()

        assert link.name == "Cloud Storage Transfer Details"
        assert link.key == "cloud_storage_transfer_details"
        assert "{project_id}" in link.format_str
        assert "{transfer_job}" in link.format_str
        assert "{transfer_operation}" in link.format_str

    def test_format_str_with_real_parameters(self):
        """Test format_str generates correct URL for real transfer operation."""
        link = CloudStorageTransferDetailsLink()

        formatted_url = link.format_str.format(
            project_id=REAL_PROJECT_ID,
            transfer_job="transferJobs",
            transfer_operation="9876543210987654321-transferJobs-1234567890123456789",
        )

        assert formatted_url == EXPECTED_DETAILS_URL
        assert formatted_url.startswith("https://console.cloud.google.com/transfer/jobs/transferJobs%2F")
        assert REAL_PROJECT_ID in formatted_url
        assert "transferJobs" in formatted_url
        assert "9876543210987654321-transferJobs-1234567890123456789" in formatted_url

    def test_extract_parts_with_real_operation_name(self):
        """Test extract_parts with real operation name."""
        transfer_operation, transfer_job = CloudStorageTransferDetailsLink.extract_parts(REAL_OPERATION_NAME)

        assert transfer_operation == "9876543210987654321-transferJobs-1234567890123456789"
        assert transfer_job == "transferJobs"

    def test_extract_parts_edge_cases(self):
        """Test extract_parts with edge cases that might occur in production."""
        edge_cases = [
            ("transferOperations/1-transferJobs-2", "1-transferJobs-2", "transferJobs"),
            ("transferOperations/abc-transferJobs-def", "abc-transferJobs-def", "transferJobs"),
            (
                "transferOperations/123_456-transferJobs-789_012",
                "123_456-transferJobs-789_012",
                "transferJobs",
            ),
        ]

        for operation_name, expected_operation, expected_job in edge_cases:
            transfer_operation, transfer_job = CloudStorageTransferDetailsLink.extract_parts(operation_name)
            assert transfer_operation == expected_operation
            assert transfer_job == expected_job

    def test_persist_method_with_real_operation_name(self):
        """Test persist method behavior with real operation name (unit test)."""
        link = CloudStorageTransferDetailsLink()

        transfer_operation, transfer_job = CloudStorageTransferDetailsLink.extract_parts(REAL_OPERATION_NAME)

        assert transfer_operation == "9876543210987654321-transferJobs-1234567890123456789"
        assert transfer_job == "transferJobs"

        formatted_url = link.format_str.format(
            project_id=REAL_PROJECT_ID,
            transfer_job=transfer_job,
            transfer_operation=transfer_operation,
        )

        assert formatted_url == EXPECTED_DETAILS_URL

    @pytest.mark.db_test
    def test_persist_with_real_operation_name(
        self, create_task_instance_of_operator, session, mock_supervisor_comms
    ):
        """Test persist method with real operation name."""
        from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

        class TestOperator(GoogleCloudBaseOperator):
            operator_extra_links = (CloudStorageTransferDetailsLink(),)

            def __init__(self, project_id: str, **kwargs):
                super().__init__(**kwargs)
                self.project_id = project_id

            @property
            def extra_links_params(self):
                return {
                    "project_id": self.project_id,
                }

            def execute(self, context):
                pass

        ti = create_task_instance_of_operator(
            TestOperator,
            dag_id="test_transfer_dag",
            task_id="test_transfer_task",
            project_id=REAL_PROJECT_ID,
        )
        session.add(ti)
        session.commit()

        from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            from airflow.sdk.execution_time.comms import XComResult

            transfer_operation, transfer_job = CloudStorageTransferDetailsLink.extract_parts(
                REAL_OPERATION_NAME
            )
            mock_supervisor_comms.send.return_value = XComResult(
                key="cloud_storage_transfer_details",
                value={
                    "project_id": REAL_PROJECT_ID,
                    "transfer_job": transfer_job,
                    "transfer_operation": transfer_operation,
                },
            )

        CloudStorageTransferDetailsLink.persist(
            context={"ti": ti, "task": ti.task},
            project_id=REAL_PROJECT_ID,
            operation_name=REAL_OPERATION_NAME,
        )

        actual_url = CloudStorageTransferDetailsLink.get_link(
            CloudStorageTransferDetailsLink(), operator=ti.task, ti_key=ti.key
        )
        assert actual_url == EXPECTED_DETAILS_URL

    @pytest.mark.db_test
    def test_persist_with_none_operation_name(
        self, create_task_instance_of_operator, session, mock_supervisor_comms
    ):
        """Test persist method with None operation name."""
        from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

        class TestOperator(GoogleCloudBaseOperator):
            operator_extra_links = (CloudStorageTransferDetailsLink(),)

            def __init__(self, project_id: str, **kwargs):
                super().__init__(**kwargs)
                self.project_id = project_id

            @property
            def extra_links_params(self):
                return {
                    "project_id": self.project_id,
                }

            def execute(self, context):
                pass

        ti = create_task_instance_of_operator(
            TestOperator,
            dag_id="test_transfer_dag",
            task_id="test_transfer_task",
            project_id=REAL_PROJECT_ID,
        )
        session.add(ti)
        session.commit()

        from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            from airflow.sdk.execution_time.comms import XComResult

            transfer_operation, transfer_job = CloudStorageTransferDetailsLink.extract_parts(None)
            mock_supervisor_comms.send.return_value = XComResult(
                key="cloud_storage_transfer_details",
                value={
                    "project_id": REAL_PROJECT_ID,
                    "transfer_job": transfer_job,
                    "transfer_operation": transfer_operation,
                },
            )

        CloudStorageTransferDetailsLink.persist(
            context={"ti": ti, "task": ti.task},
            project_id=REAL_PROJECT_ID,
            operation_name=None,
        )

        actual_url = CloudStorageTransferDetailsLink.get_link(
            CloudStorageTransferDetailsLink(), operator=ti.task, ti_key=ti.key
        )
        expected_url = f"https://console.cloud.google.com/transfer/jobs/transferJobs%2F/runs/transferOperations%2F?project={REAL_PROJECT_ID}"
        assert actual_url == expected_url

    @pytest.mark.db_test
    def test_persist_with_empty_operation_name(
        self, create_task_instance_of_operator, session, mock_supervisor_comms
    ):
        """Test persist method with empty operation name."""
        from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

        class TestOperator(GoogleCloudBaseOperator):
            operator_extra_links = (CloudStorageTransferDetailsLink(),)

            def __init__(self, project_id: str, **kwargs):
                super().__init__(**kwargs)
                self.project_id = project_id

            @property
            def extra_links_params(self):
                return {
                    "project_id": self.project_id,
                }

            def execute(self, context):
                pass

        ti = create_task_instance_of_operator(
            TestOperator,
            dag_id="test_transfer_dag",
            task_id="test_transfer_task",
            project_id=REAL_PROJECT_ID,
        )
        session.add(ti)
        session.commit()

        from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            from airflow.sdk.execution_time.comms import XComResult

            transfer_operation, transfer_job = CloudStorageTransferDetailsLink.extract_parts("")
            mock_supervisor_comms.send.return_value = XComResult(
                key="cloud_storage_transfer_details",
                value={
                    "project_id": REAL_PROJECT_ID,
                    "transfer_job": transfer_job,
                    "transfer_operation": transfer_operation,
                },
            )

        CloudStorageTransferDetailsLink.persist(
            context={"ti": ti, "task": ti.task},
            project_id=REAL_PROJECT_ID,
            operation_name="",
        )

        actual_url = CloudStorageTransferDetailsLink.get_link(
            CloudStorageTransferDetailsLink(), operator=ti.task, ti_key=ti.key
        )
        expected_url = f"https://console.cloud.google.com/transfer/jobs/transferJobs%2F/runs/transferOperations%2F?project={REAL_PROJECT_ID}"
        assert actual_url == expected_url


class TestIntegrationScenarios:
    """Integration tests with real-world scenarios."""

    def test_complete_transfer_workflow_links(self):
        """Test all link types work together in a complete transfer workflow."""
        list_link = CloudStorageTransferListLink()
        list_url = list_link.format_str.format(project_id=REAL_PROJECT_ID)

        job_link = CloudStorageTransferJobLink()
        job_url = job_link.format_str.format(project_id=REAL_PROJECT_ID, transfer_job=REAL_TRANSFER_JOB)

        details_link = CloudStorageTransferDetailsLink()
        details_url = details_link.format_str.format(
            project_id=REAL_PROJECT_ID,
            transfer_job="transferJobs",
            transfer_operation="9876543210987654321-transferJobs-1234567890123456789",
        )

        assert all(
            url.startswith("https://console.cloud.google.com/transfer")
            for url in [list_url, job_url, details_url]
        )
        assert REAL_PROJECT_ID in list_url
        assert REAL_PROJECT_ID in job_url
        assert REAL_PROJECT_ID in details_url
        assert REAL_TRANSFER_JOB in job_url
        assert "transferJobs" in details_url
        assert "9876543210987654321-transferJobs-1234567890123456789" in details_url

    def test_url_consistency_across_link_types(self):
        """Test that URLs are consistent across different link types."""
        base_url = "https://console.cloud.google.com/transfer"

        list_link = CloudStorageTransferListLink()
        job_link = CloudStorageTransferJobLink()
        details_link = CloudStorageTransferDetailsLink()

        assert list_link.format_str.startswith(base_url)
        assert job_link.format_str.startswith(base_url)
        assert details_link.format_str.startswith(base_url)
