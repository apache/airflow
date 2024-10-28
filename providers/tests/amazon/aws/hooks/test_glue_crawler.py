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

from copy import deepcopy
from typing import TYPE_CHECKING
from unittest import mock

from moto import mock_aws
from moto.core import DEFAULT_ACCOUNT_ID

from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook

if TYPE_CHECKING:
    from unittest.mock import MagicMock

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
            {
                "ConnectionName": "test-mongo-conn",
                "Path": "test_db/test_collection",
                "ScanAll": True,
            }
        ],
        "DynamoDBTargets": [
            {"Path": "test_db/test_table", "scanAll": True, "scanRate": 123.0}
        ],
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
    "Tags": {"test": "foo", "bar": "test"},
}


class TestGlueCrawlerHook:
    def setup_method(self):
        self.hook = GlueCrawlerHook(aws_conn_id="aws_default")
        self.crawler_arn = f"arn:aws:glue:{self.hook.conn_region_name}:{DEFAULT_ACCOUNT_ID}:crawler/{mock_crawler_name}"

    def test_init(self):
        assert self.hook.aws_conn_id == "aws_default"

    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_has_crawler(self, mock_get_conn):
        assert self.hook.has_crawler(mock_crawler_name) is True
        mock_get_conn.return_value.get_crawler.assert_called_once_with(
            Name=mock_crawler_name
        )

    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_has_crawler_crawled_doesnt_exists(self, mock_get_conn):
        class MockException(Exception):
            pass

        mock_get_conn.return_value.exceptions.EntityNotFoundException = MockException
        mock_get_conn.return_value.get_crawler.side_effect = MockException("AAA")
        assert self.hook.has_crawler(mock_crawler_name) is False
        mock_get_conn.return_value.get_crawler.assert_called_once_with(
            Name=mock_crawler_name
        )

    @mock_aws
    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_update_crawler_needed(self, mock_get_conn):
        mock_get_conn.return_value.get_crawler.return_value = {"Crawler": mock_config}

        mock_config_two = deepcopy(mock_config)
        mock_config_two["Role"] = "test-2-role"
        mock_config_two.pop("Tags")
        assert self.hook.update_crawler(**mock_config_two) is True
        mock_get_conn.return_value.get_crawler.assert_called_once_with(
            Name=mock_crawler_name
        )
        mock_get_conn.return_value.update_crawler.assert_called_once_with(
            **mock_config_two
        )

    @mock_aws
    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_update_crawler_missing_keys(self, mock_get_conn):
        mock_config_missing_configuration = deepcopy(mock_config)
        mock_config_missing_configuration.pop("Configuration")
        mock_get_conn.return_value.get_crawler.return_value = {
            "Crawler": mock_config_missing_configuration
        }

        mock_config_two = deepcopy(mock_config)
        mock_config_two.pop("Tags")
        assert self.hook.update_crawler(**mock_config_two) is True
        mock_get_conn.return_value.get_crawler.assert_called_once_with(
            Name=mock_crawler_name
        )
        mock_get_conn.return_value.update_crawler.assert_called_once_with(
            **mock_config_two
        )

    @mock_aws
    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_update_tags_not_needed(self, mock_get_conn):
        mock_get_conn.return_value.get_crawler.return_value = {"Crawler": mock_config}
        mock_get_conn.return_value.get_tags.return_value = {"Tags": mock_config["Tags"]}

        assert self.hook.update_tags(mock_crawler_name, mock_config["Tags"]) is False
        mock_get_conn.return_value.get_tags.assert_called_once_with(
            ResourceArn=self.crawler_arn
        )
        mock_get_conn.return_value.tag_resource.assert_not_called()
        mock_get_conn.return_value.untag_resource.assert_not_called()

    @mock_aws
    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_remove_all_tags(self, mock_get_conn):
        mock_get_conn.return_value.get_crawler.return_value = {"Crawler": mock_config}
        mock_get_conn.return_value.get_tags.return_value = {"Tags": mock_config["Tags"]}

        assert self.hook.update_tags(mock_crawler_name, {}) is True
        mock_get_conn.return_value.get_tags.assert_called_once_with(
            ResourceArn=self.crawler_arn
        )
        mock_get_conn.return_value.tag_resource.assert_not_called()
        mock_get_conn.return_value.untag_resource.assert_called_once_with(
            ResourceArn=self.crawler_arn, TagsToRemove=["test", "bar"]
        )

    @mock_aws
    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_update_missing_tags(self, mock_get_conn):
        mock_config_missing_tags = deepcopy(mock_config)
        mock_config_missing_tags.pop("Tags")
        mock_get_conn.return_value.get_crawler.return_value = {
            "Crawler": mock_config_missing_tags
        }

        assert self.hook.update_crawler(**mock_config_missing_tags) is False
        mock_get_conn.return_value.get_tags.assert_not_called()
        mock_get_conn.return_value.tag_resource.assert_not_called()
        mock_get_conn.return_value.untag_resource.assert_not_called()

    @mock_aws
    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_replace_tag(self, mock_get_conn):
        mock_get_conn.return_value.get_crawler.return_value = {"Crawler": mock_config}
        mock_get_conn.return_value.get_tags.return_value = {"Tags": mock_config["Tags"]}

        assert (
            self.hook.update_tags(mock_crawler_name, {"test": "bla", "bar": "test"})
            is True
        )
        mock_get_conn.return_value.get_tags.assert_called_once_with(
            ResourceArn=self.crawler_arn
        )
        mock_get_conn.return_value.untag_resource.assert_not_called()
        mock_get_conn.return_value.tag_resource.assert_called_once_with(
            ResourceArn=self.crawler_arn, TagsToAdd={"test": "bla"}
        )

    @mock_aws
    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_update_crawler_not_needed(self, mock_get_conn):
        mock_get_conn.return_value.get_crawler.return_value = {"Crawler": mock_config}
        assert self.hook.update_crawler(**mock_config) is False
        mock_get_conn.return_value.get_crawler.assert_called_once_with(
            Name=mock_crawler_name
        )

    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_create_crawler(self, mock_get_conn):
        mock_get_conn.return_value.create_crawler.return_value = {
            "Crawler": {"Name": mock_crawler_name}
        }
        glue_crawler = self.hook.create_crawler(**mock_config)
        assert "Crawler" in glue_crawler
        assert "Name" in glue_crawler["Crawler"]
        assert glue_crawler["Crawler"]["Name"] == mock_crawler_name

    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_start_crawler(self, mock_get_conn):
        result = self.hook.start_crawler(mock_crawler_name)
        assert result == mock_get_conn.return_value.start_crawler.return_value

        mock_get_conn.return_value.start_crawler.assert_called_once_with(
            Name=mock_crawler_name
        )

    @mock.patch.object(GlueCrawlerHook, "get_crawler")
    @mock.patch.object(GlueCrawlerHook, "get_conn")
    @mock.patch.object(GlueCrawlerHook, "get_waiter")
    def test_wait_for_crawler_completion_instant_ready(
        self, _, mock_get_conn: MagicMock, mock_get_crawler: MagicMock
    ):
        mock_get_crawler.return_value = {
            "State": "READY",
            "LastCrawl": {"Status": "MOCK_STATUS"},
        }
        mock_get_conn.return_value.get_crawler_metrics.return_value = {
            "CrawlerMetricsList": [
                {
                    "LastRuntimeSeconds": "TEST-A",
                    "MedianRuntimeSeconds": "TEST-B",
                    "TablesCreated": "TEST-C",
                    "TablesUpdated": "TEST-D",
                    "TablesDeleted": "TEST-E",
                }
            ]
        }
        assert self.hook.wait_for_crawler_completion(mock_crawler_name) == "MOCK_STATUS"
        mock_get_conn.assert_has_calls(
            [
                mock.call(),
                mock.call().get_crawler_metrics(CrawlerNameList=[mock_crawler_name]),
            ]
        )
        mock_get_crawler.assert_called_once_with(mock_crawler_name)
