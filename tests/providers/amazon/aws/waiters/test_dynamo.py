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
import pytest
from botocore.exceptions import WaiterError

from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook

TEST_IMPORT_ARN = "arn:aws:dynamodb:us-east-1:255683865591:table/test-table/import/01662190284205-aa94decf"


class TestCustomDynamoDBServiceWaiters:
    EXPORT_STATUS_COMPLETED = "COMPLETED"
    EXPORT_STATUS_FAILED = "FAILED"
    EXPORT_STATUS_IN_PROGRESS = "IN_PROGRESS"

    IMPORT_STATUS_FAILED = ("CANCELLING", "CANCELLED", "FAILED")
    IMPORT_STATUS_COMPLETED = "COMPLETED"
    IMPORT_STATUS_IN_PROGRESS = "IN_PROGRESS"

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.resource = boto3.resource("dynamodb", region_name="eu-west-3")
        monkeypatch.setattr(DynamoDBHook, "conn", self.resource)
        self.client = self.resource.meta.client

    @pytest.fixture
    def mock_describe_export(self):
        """Mock ``DynamoDBHook.Client.describe_export`` method."""
        with mock.patch.object(self.client, "describe_export") as m:
            yield m

    @pytest.fixture
    def mock_describe_import(self):
        """Mock ``DynamoDBHook.Client.describe_import`` method."""
        with mock.patch.object(self.client, "describe_import") as m:
            yield m

    def test_service_waiters(self):
        hook_waiters = DynamoDBHook(aws_conn_id=None).list_waiters()
        assert "export_table" in hook_waiters
        assert "import_table" in hook_waiters

    @staticmethod
    def describe_export(status: str):
        """
        Helper function for generate minimal DescribeExport response for single job.
        https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeExport.html
        """
        return {"ExportDescription": {"ExportStatus": status}}

    @staticmethod
    def describe_import(status: str):
        """
        Helper function for generate minimal DescribeImport response for single job.
        https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeImport.html
        """
        return {"ImportTableDescription": {"ImportStatus": status}}

    def test_export_table_to_point_in_time_completed(self, mock_describe_export):
        """Test state transition from `in progress` to `completed` during init."""
        waiter = DynamoDBHook(aws_conn_id=None).get_waiter("export_table")
        mock_describe_export.side_effect = [
            self.describe_export(self.EXPORT_STATUS_IN_PROGRESS),
            self.describe_export(self.EXPORT_STATUS_COMPLETED),
        ]
        waiter.wait(
            ExportArn="LoremIpsumissimplydummytextoftheprintingandtypesettingindustry",
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
        )

    def test_export_table_to_point_in_time_failed(self, mock_describe_export):
        """Test state transition from `in progress` to `failed` during init."""
        with mock.patch("boto3.client") as client:
            client.return_value = self.client
            mock_describe_export.side_effect = [
                self.describe_export(self.EXPORT_STATUS_IN_PROGRESS),
                self.describe_export(self.EXPORT_STATUS_FAILED),
            ]
            waiter = DynamoDBHook(aws_conn_id=None).get_waiter("export_table", client=self.client)
            with pytest.raises(WaiterError, match='we matched expected path: "FAILED"'):
                waiter.wait(
                    ExportArn="LoremIpsumissimplydummytextoftheprintingandtypesettingindustry",
                    WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
                )

    def test_import_table_completed(self, mock_describe_import):
        waiter = DynamoDBHook(aws_conn_id=None).get_waiter("import_table")
        mock_describe_import.side_effect = [
            self.describe_import(self.IMPORT_STATUS_IN_PROGRESS),
            self.describe_import(self.IMPORT_STATUS_COMPLETED),
        ]
        waiter.wait(
            ImportArn=TEST_IMPORT_ARN,
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
        )

    @pytest.mark.parametrize(
        "status",
        [
            pytest.param(IMPORT_STATUS_FAILED[0]),
            pytest.param(IMPORT_STATUS_FAILED[1]),
            pytest.param(IMPORT_STATUS_FAILED[2]),
        ],
    )
    def test_import_table_failed(self, status, mock_describe_import):
        waiter = DynamoDBHook(aws_conn_id=None).get_waiter("import_table")
        mock_describe_import.side_effect = [
            self.describe_import(self.EXPORT_STATUS_IN_PROGRESS),
            self.describe_import(status),
        ]
        with pytest.raises(WaiterError, match=f'we matched expected path: "{status}"'):
            waiter.wait(ImportArn=TEST_IMPORT_ARN, WaiterConfig={"Delay": 0.01, "MaxAttempts": 3})
