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
"""Tests for BigQuery links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.bigquery import (
    BIGQUERY_DATASET_LINK,
    BIGQUERY_JOB_DETAIL_LINK,
    BIGQUERY_TABLE_LINK,
    BigQueryDatasetLink,
    BigQueryJobDetailLink,
    BigQueryTableLink,
)

TEST_PROJECT_ID = "test-project"
TEST_DATASET_ID = "test-dataset"
TEST_TABLE_ID = "test-table"
TEST_JOB_ID = "test-job-id"
TEST_LOCATION = "US"


class TestBigQueryDatasetLink:
    def test_class_attributes(self):
        assert BigQueryDatasetLink.key == "bigquery_dataset"
        assert BigQueryDatasetLink.name == "BigQuery Dataset"
        assert BigQueryDatasetLink.format_str == BIGQUERY_DATASET_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        BigQueryDatasetLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="bigquery_dataset",
            value={"project_id": TEST_PROJECT_ID, "dataset_id": TEST_DATASET_ID},
        )

    def test_format_link(self):
        link = BigQueryDatasetLink()
        result = link._format_link(project_id=TEST_PROJECT_ID, dataset_id=TEST_DATASET_ID)
        expected = BASE_LINK + BIGQUERY_DATASET_LINK.format(
            project_id=TEST_PROJECT_ID, dataset_id=TEST_DATASET_ID
        )
        assert result == expected


class TestBigQueryTableLink:
    def test_class_attributes(self):
        assert BigQueryTableLink.key == "bigquery_table"
        assert BigQueryTableLink.name == "BigQuery Table"
        assert BigQueryTableLink.format_str == BIGQUERY_TABLE_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        BigQueryTableLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            dataset_id=TEST_DATASET_ID,
            table_id=TEST_TABLE_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="bigquery_table",
            value={
                "project_id": TEST_PROJECT_ID,
                "dataset_id": TEST_DATASET_ID,
                "table_id": TEST_TABLE_ID,
            },
        )

    def test_format_link(self):
        link = BigQueryTableLink()
        result = link._format_link(
            project_id=TEST_PROJECT_ID, dataset_id=TEST_DATASET_ID, table_id=TEST_TABLE_ID
        )
        expected = BASE_LINK + BIGQUERY_TABLE_LINK.format(
            project_id=TEST_PROJECT_ID, dataset_id=TEST_DATASET_ID, table_id=TEST_TABLE_ID
        )
        assert result == expected


class TestBigQueryJobDetailLink:
    def test_class_attributes(self):
        assert BigQueryJobDetailLink.key == "bigquery_job_detail"
        assert BigQueryJobDetailLink.name == "BigQuery Job Detail"
        assert BigQueryJobDetailLink.format_str == BIGQUERY_JOB_DETAIL_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        BigQueryJobDetailLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            job_id=TEST_JOB_ID,
            location=TEST_LOCATION,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="bigquery_job_detail",
            value={
                "project_id": TEST_PROJECT_ID,
                "job_id": TEST_JOB_ID,
                "location": TEST_LOCATION,
            },
        )

    def test_format_link(self):
        link = BigQueryJobDetailLink()
        result = link._format_link(project_id=TEST_PROJECT_ID, job_id=TEST_JOB_ID, location=TEST_LOCATION)
        expected = BASE_LINK + BIGQUERY_JOB_DETAIL_LINK.format(
            project_id=TEST_PROJECT_ID, job_id=TEST_JOB_ID, location=TEST_LOCATION
        )
        assert result == expected
