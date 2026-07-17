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

from collections.abc import Generator
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.providers.amazon.aws.hooks.sts import StsHook
from airflow.providers.amazon.aws.operators.glue_crawler import (
    GlueCrawlerCreateOperator,
    GlueCrawlerDeleteOperator,
    GlueCrawlerOperator,
    GlueCrawlerRunOperator,
    GlueCrawlerUpdateOperator,
)
from airflow.providers.common.compat.sdk import AirflowException

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection

mock_crawler_name = "test-crawler"
mock_role_name = "test-role"
mock_config = {
    "Name": mock_crawler_name,
    "Description": "Test glue crawler from Airflow",
    "DatabaseName": "test_db",
    "Role": mock_role_name,
    "Targets": {
        "S3Targets": [
            {
                "Path": "s3://test-glue-crawler/foo/",
                "Exclusions": [
                    "s3://test-glue-crawler/bar/",
                ],
                "ConnectionName": "test-s3-conn",
            }
        ],
        "JdbcTargets": [
            {
                "ConnectionName": "test-jdbc-conn",
                "Path": "test_db/test_table>",
                "Exclusions": [
                    "string",
                ],
            }
        ],
        "MongoDBTargets": [
            {"ConnectionName": "test-mongo-conn", "Path": "test_db/test_collection", "ScanAll": True}
        ],
        "DynamoDBTargets": [{"Path": "test_db/test_table", "scanAll": True, "scanRate": 123.0}],
        "CatalogTargets": [
            {
                "DatabaseName": "test_glue_db",
                "Tables": [
                    "test",
                ],
            }
        ],
    },
    "Classifiers": ["test-classifier"],
    "TablePrefix": "test",
    "SchemaChangePolicy": {
        "UpdateBehavior": "UPDATE_IN_DATABASE",
        "DeleteBehavior": "DEPRECATE_IN_DATABASE",
    },
    "RecrawlPolicy": {"RecrawlBehavior": "CRAWL_EVERYTHING"},
    "LineageConfiguration": "ENABLE",
    "Configuration": """
    {
        "Version": 1.0,
        "CrawlerOutput": {
            "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" }
        }
    }
    """,
    "SecurityConfiguration": "test",
    "Tags": {"test": "foo"},
}


class TestGlueCrawlerCreateOperator:
    @mock.patch.object(GlueCrawlerHook, "create_crawler", autospec=True)
    def test_execute(self, mock_create_crawler):
        op = GlueCrawlerCreateOperator(task_id="create_crawler", config=mock_config)

        result = op.execute({})

        assert result == mock_crawler_name
        mock_create_crawler.assert_called_once_with(op.hook, **mock_config)

    def test_template_fields(self):
        op = GlueCrawlerCreateOperator(task_id="create_crawler", config=mock_config)

        validate_template_fields(op)


class TestGlueCrawlerUpdateOperator:
    @mock.patch.object(GlueCrawlerHook, "update_crawler", autospec=True)
    def test_execute(self, mock_update_crawler):
        op = GlueCrawlerUpdateOperator(task_id="update_crawler", config=mock_config)

        result = op.execute({})

        assert result == mock_crawler_name
        mock_update_crawler.assert_called_once_with(op.hook, **mock_config)

    def test_template_fields(self):
        op = GlueCrawlerUpdateOperator(task_id="update_crawler", config=mock_config)

        validate_template_fields(op)


class TestGlueCrawlerRunOperator:
    @mock.patch.object(GlueCrawlerHook, "wait_for_crawler_completion", autospec=True)
    @mock.patch.object(GlueCrawlerHook, "start_crawler", autospec=True)
    def test_execute_waits_for_completion_by_default(self, mock_start_crawler, mock_wait):
        op = GlueCrawlerRunOperator(task_id="run_crawler", crawler_name=mock_crawler_name)

        result = op.execute({})

        assert result == mock_crawler_name
        mock_start_crawler.assert_called_once_with(op.hook, mock_crawler_name)
        mock_wait.assert_called_once_with(op.hook, crawler_name=mock_crawler_name, poll_interval=5)

    @mock.patch.object(GlueCrawlerHook, "wait_for_crawler_completion", autospec=True)
    @mock.patch.object(GlueCrawlerHook, "start_crawler", autospec=True)
    def test_execute_without_waiting(self, mock_start_crawler, mock_wait):
        op = GlueCrawlerRunOperator(
            task_id="run_crawler", crawler_name=mock_crawler_name, wait_for_completion=False
        )

        result = op.execute({})

        assert result == mock_crawler_name
        mock_start_crawler.assert_called_once_with(op.hook, mock_crawler_name)
        mock_wait.assert_not_called()

    @mock.patch.object(GlueCrawlerHook, "wait_for_crawler_completion", autospec=True)
    @mock.patch.object(GlueCrawlerHook, "start_crawler", autospec=True)
    def test_execute_defers(self, mock_start_crawler, mock_wait):
        op = GlueCrawlerRunOperator(
            task_id="run_crawler",
            crawler_name=mock_crawler_name,
            poll_interval=10,
            deferrable=True,
            aws_conn_id="fake-conn-id",
            region_name="eu-west-2",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        op.defer = mock.MagicMock(spec=op.defer)

        result = op.execute({})

        assert result == mock_crawler_name
        mock_start_crawler.assert_called_once_with(op.hook, mock_crawler_name)
        mock_wait.assert_not_called()
        op.defer.assert_called_once()
        trigger = op.defer.call_args.kwargs["trigger"]
        _, trigger_kwargs = trigger.serialize()
        assert trigger_kwargs == {
            "crawler_name": mock_crawler_name,
            "waiter_delay": 10,
            "waiter_max_attempts": 1500,
            "aws_conn_id": "fake-conn-id",
            "region_name": "eu-west-2",
            "verify": False,
            "botocore_config": {"read_timeout": 42},
        }
        assert op.defer.call_args.kwargs["method_name"] == "execute_complete"

    @mock.patch.object(GlueCrawlerHook, "wait_for_crawler_completion", autospec=True)
    @mock.patch.object(GlueCrawlerHook, "start_crawler", autospec=True)
    def test_execute_waits_when_crawler_is_already_running(self, mock_start_crawler, mock_wait):
        mock_start_crawler.side_effect = ClientError(
            error_response={"Error": {"Code": "CrawlerRunningException", "Message": "Already running"}},
            operation_name="StartCrawler",
        )
        op = GlueCrawlerRunOperator(
            task_id="run_crawler",
            crawler_name=mock_crawler_name,
            fail_on_already_running=False,
        )

        result = op.execute({})

        assert result == mock_crawler_name
        mock_wait.assert_called_once_with(op.hook, crawler_name=mock_crawler_name, poll_interval=5)

    @pytest.mark.parametrize("error_code", ["CrawlerRunningException", "EntityNotFoundException"])
    @mock.patch.object(GlueCrawlerHook, "start_crawler", autospec=True)
    def test_execute_propagates_client_error(self, mock_start_crawler, error_code):
        mock_start_crawler.side_effect = ClientError(
            error_response={"Error": {"Code": error_code, "Message": "error"}},
            operation_name="StartCrawler",
        )
        op = GlueCrawlerRunOperator(task_id="run_crawler", crawler_name=mock_crawler_name)

        with pytest.raises(ClientError) as exc_info:
            op.execute({})

        assert exc_info.value.response["Error"]["Code"] == error_code

    def test_execute_complete(self):
        op = GlueCrawlerRunOperator(task_id="run_crawler", crawler_name=mock_crawler_name)

        assert op.execute_complete({}, {"status": "success"}) == mock_crawler_name

    def test_execute_complete_raises_for_failure(self):
        op = GlueCrawlerRunOperator(task_id="run_crawler", crawler_name=mock_crawler_name)

        with pytest.raises(AirflowException, match="Error in glue crawl"):
            op.execute_complete({}, {"status": "error"})

    def test_template_fields(self):
        op = GlueCrawlerRunOperator(task_id="run_crawler", crawler_name=mock_crawler_name)

        validate_template_fields(op)


class TestGlueCrawlerDeleteOperator:
    @mock.patch.object(GlueCrawlerHook, "glue_client", new_callable=mock.PropertyMock)
    def test_execute(self, mock_glue_client):
        client = mock.MagicMock(spec=["delete_crawler"])
        mock_glue_client.return_value = client
        op = GlueCrawlerDeleteOperator(task_id="delete_crawler", crawler_name=mock_crawler_name)

        result = op.execute({})

        assert result is None
        client.delete_crawler.assert_called_once_with(Name=mock_crawler_name)

    def test_template_fields(self):
        op = GlueCrawlerDeleteOperator(task_id="delete_crawler", crawler_name=mock_crawler_name)

        validate_template_fields(op)


class TestGlueCrawlerOperator:
    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(GlueCrawlerHook, "get_conn") as _conn:
            _conn.create_crawler.return_value = mock_crawler_name
            yield _conn

    @pytest.fixture
    def crawler_hook(self) -> Generator[GlueCrawlerHook, None, None]:
        with mock_aws():
            hook = GlueCrawlerHook(aws_conn_id="aws_default")
            yield hook

    def setup_method(self):
        with pytest.warns(AirflowProviderDeprecationWarning, match="GlueCrawlerOperator is deprecated"):
            self.op = GlueCrawlerOperator(task_id="test_glue_crawler_operator", config=mock_config)

    def test_init(self):
        with pytest.warns(AirflowProviderDeprecationWarning):
            op = GlueCrawlerOperator(
                task_id="test_glue_crawler_operator",
                aws_conn_id="fake-conn-id",
                region_name="eu-west-2",
                verify=True,
                botocore_config={"read_timeout": 42},
                config=mock_config,
            )

        assert op.hook.client_type == "glue"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "eu-west-2"
        assert op.hook._verify is True
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        with pytest.warns(AirflowProviderDeprecationWarning):
            op = GlueCrawlerOperator(task_id="test_glue_crawler_operator", config=mock_config)

        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    @mock.patch.object(GlueCrawlerHook, "glue_client")
    @mock.patch.object(StsHook, "get_account_number")
    def test_execute_update_and_start_crawler(self, sts_mock, mock_glue_client):
        sts_mock.get_account_number.return_value = 123456789012
        mock_glue_client.get_crawler.return_value = {"Crawler": {}}
        self.op.wait_for_completion = False
        crawler_name = self.op.execute({})

        mock_glue_client.get_crawler.call_count = 2
        mock_glue_client.update_crawler.call_count = 1
        mock_glue_client.start_crawler.call_count = 1
        assert crawler_name == mock_crawler_name

    @mock.patch.object(GlueCrawlerHook, "has_crawler")
    @mock.patch.object(GlueCrawlerHook, "glue_client")
    def test_execute_create_and_start_crawler(self, mock_glue_client, mock_has_crawler):
        mock_has_crawler.return_value = False
        mock_glue_client.create_crawler.return_value = {}
        self.op.wait_for_completion = False
        crawler_name = self.op.execute({})

        assert crawler_name == mock_crawler_name
        mock_glue_client.create_crawler.assert_called_once()

    @pytest.mark.parametrize(
        ("wait_for_completion", "deferrable"),
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(GlueCrawlerHook, "get_waiter")
    def test_crawler_wait_combinations(self, _, wait_for_completion, deferrable, mock_conn, crawler_hook):
        self.op.defer = mock.MagicMock()
        self.op.wait_for_completion = wait_for_completion
        self.op.deferrable = deferrable

        response = self.op.execute({})

        assert response == mock_crawler_name
        assert crawler_hook.get_waiter.call_count == wait_for_completion
        assert self.op.defer.call_count == deferrable

    def test_template_fields(self):
        validate_template_fields(self.op)

    def test_execute_complete_returns_name_from_config(self):
        self.op.crawler_name = "unrendered-name"

        result = self.op.execute_complete({}, {"status": "success"})

        assert result == mock_config["Name"]

    @mock.patch.object(GlueCrawlerHook, "wait_for_crawler_completion")
    @mock.patch.object(GlueCrawlerHook, "start_crawler")
    @mock.patch.object(GlueCrawlerHook, "update_crawler")
    @mock.patch.object(GlueCrawlerHook, "has_crawler")
    def test_execute_crawler_running_on_start(self, mock_has_crawler, mock_update, mock_start, mock_wait):
        """CrawlerRunningException on start_crawler should be caught when fail_on_already_running=False."""
        mock_has_crawler.return_value = True
        mock_start.side_effect = ClientError(
            error_response={"Error": {"Code": "CrawlerRunningException", "Message": "Already running"}},
            operation_name="StartCrawler",
        )
        self.op.fail_on_already_running = False

        crawler_name = self.op.execute({})

        assert crawler_name == mock_crawler_name
        mock_update.assert_called_once()
        mock_start.assert_called_once_with(mock_crawler_name)
        mock_wait.assert_called_once_with(crawler_name=mock_crawler_name, poll_interval=5)

    @mock.patch.object(GlueCrawlerHook, "start_crawler")
    @mock.patch.object(GlueCrawlerHook, "update_crawler")
    @mock.patch.object(GlueCrawlerHook, "has_crawler")
    def test_execute_crawler_running_on_update(self, mock_has_crawler, mock_update, mock_start):
        """CrawlerRunningException on update_crawler should be caught when fail_on_already_running=False."""
        mock_has_crawler.return_value = True
        mock_update.side_effect = ClientError(
            error_response={"Error": {"Code": "CrawlerRunningException", "Message": "Already running"}},
            operation_name="UpdateCrawler",
        )
        self.op.fail_on_already_running = False
        self.op.wait_for_completion = False

        crawler_name = self.op.execute({})

        assert crawler_name == mock_crawler_name
        mock_update.assert_called_once()
        mock_start.assert_called_once_with(mock_crawler_name)

    @mock.patch.object(GlueCrawlerHook, "start_crawler")
    @mock.patch.object(GlueCrawlerHook, "update_crawler")
    @mock.patch.object(GlueCrawlerHook, "has_crawler")
    def test_execute_other_client_error_on_start_raises(self, mock_has_crawler, mock_update, mock_start):
        """Non-CrawlerRunningException ClientError on start_crawler should propagate."""
        mock_has_crawler.return_value = True
        mock_start.side_effect = ClientError(
            error_response={"Error": {"Code": "EntityNotFoundException", "Message": "Not found"}},
            operation_name="StartCrawler",
        )
        self.op.wait_for_completion = False

        with pytest.raises(ClientError) as exc_info:
            self.op.execute({})

        assert exc_info.value.response["Error"]["Code"] == "EntityNotFoundException"

    @mock.patch.object(GlueCrawlerHook, "update_crawler")
    @mock.patch.object(GlueCrawlerHook, "has_crawler")
    def test_execute_other_client_error_on_update_raises(self, mock_has_crawler, mock_update):
        """Non-CrawlerRunningException ClientError on update_crawler should propagate."""
        mock_has_crawler.return_value = True
        mock_update.side_effect = ClientError(
            error_response={"Error": {"Code": "InvalidInputException", "Message": "Bad config"}},
            operation_name="UpdateCrawler",
        )

        with pytest.raises(ClientError) as exc_info:
            self.op.execute({})

        assert exc_info.value.response["Error"]["Code"] == "InvalidInputException"

    @mock.patch.object(GlueCrawlerHook, "start_crawler")
    @mock.patch.object(GlueCrawlerHook, "update_crawler")
    @mock.patch.object(GlueCrawlerHook, "has_crawler")
    def test_execute_crawler_running_on_start_raises_when_fail_on_already_running(
        self, mock_has_crawler, mock_update, mock_start
    ):
        """CrawlerRunningException on start_crawler re-raises by default (fail_on_already_running=True)."""
        mock_has_crawler.return_value = True
        mock_start.side_effect = ClientError(
            error_response={"Error": {"Code": "CrawlerRunningException", "Message": "Already running"}},
            operation_name="StartCrawler",
        )
        self.op.wait_for_completion = False

        with pytest.raises(ClientError) as exc_info:
            self.op.execute({})

        assert exc_info.value.response["Error"]["Code"] == "CrawlerRunningException"

    @mock.patch.object(GlueCrawlerHook, "start_crawler")
    @mock.patch.object(GlueCrawlerHook, "update_crawler")
    @mock.patch.object(GlueCrawlerHook, "has_crawler")
    def test_execute_crawler_running_on_update_raises_when_fail_on_already_running(
        self, mock_has_crawler, mock_update, mock_start
    ):
        """CrawlerRunningException on update_crawler re-raises by default (fail_on_already_running=True)."""
        mock_has_crawler.return_value = True
        mock_update.side_effect = ClientError(
            error_response={"Error": {"Code": "CrawlerRunningException", "Message": "Already running"}},
            operation_name="UpdateCrawler",
        )
        self.op.fail_on_already_running = True

        with pytest.raises(ClientError) as exc_info:
            self.op.execute({})

        assert exc_info.value.response["Error"]["Code"] == "CrawlerRunningException"
        mock_start.assert_not_called()
