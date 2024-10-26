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

from typing import TYPE_CHECKING, Generator
from unittest import mock

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.providers.amazon.aws.hooks.sts import StsHook
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

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
        self.op = GlueCrawlerOperator(task_id="test_glue_crawler_operator", config=mock_config)

    def test_init(self):
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
        "wait_for_completion, deferrable",
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
