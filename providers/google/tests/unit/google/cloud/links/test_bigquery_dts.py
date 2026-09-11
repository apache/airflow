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
"""Tests for BigQuery Data Transfer links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.bigquery_dts import (
    BIGQUERY_DTS_LINK,
    BigQueryDataTransferConfigLink,
)

TEST_PROJECT_ID = "test-project"
TEST_REGION = "us-central1"
TEST_CONFIG_ID = "test-config-id"


class TestBigQueryDataTransferConfigLink:
    def test_class_attributes(self):
        assert BigQueryDataTransferConfigLink.key == "bigquery_dts_config"
        assert BigQueryDataTransferConfigLink.name == "BigQuery Data Transfer Config"
        assert BigQueryDataTransferConfigLink.format_str == BIGQUERY_DTS_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        BigQueryDataTransferConfigLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            config_id=TEST_CONFIG_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="bigquery_dts_config",
            value={
                "project_id": TEST_PROJECT_ID,
                "region": TEST_REGION,
                "config_id": TEST_CONFIG_ID,
            },
        )

    def test_format_link(self):
        link = BigQueryDataTransferConfigLink()
        result = link._format_link(project_id=TEST_PROJECT_ID, region=TEST_REGION, config_id=TEST_CONFIG_ID)
        expected = BASE_LINK + BIGQUERY_DTS_LINK.format(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, config_id=TEST_CONFIG_ID
        )
        assert result == expected
