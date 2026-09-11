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

"""Tests for Dataflow links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.dataflow import (
    DATAFLOW_JOB_LINK,
    DATAFLOW_PIPELINE_LINK,
    DataflowJobLink,
    DataflowPipelineLink,
)

TEST_JOB_ID = "test-job-id"
TEST_LOCATION = "test-location"
TEST_PIPELINE_NAME = "test-pipeline-name"
TEST_PROJECT_ID = "test-project-id"
TEST_REGION = "test-region"


class TestDataflowJobLink:
    def test_class_attributes(self):
        assert DataflowJobLink.key == "dataflow_job_config"
        assert DataflowJobLink.name == "Dataflow Job"
        assert DataflowJobLink.format_str == DATAFLOW_JOB_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataflowJobLink.persist(
            context=mock_context,
            job_id=TEST_JOB_ID,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="dataflow_job_config",
            value={"job_id": TEST_JOB_ID, "project_id": TEST_PROJECT_ID, "region": TEST_REGION},
        )

    def test_format_link(self):
        link = DataflowJobLink()

        result = link._format_link(job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, region=TEST_REGION)

        assert result == BASE_LINK + DATAFLOW_JOB_LINK.format(
            job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, region=TEST_REGION
        )


class TestDataflowPipelineLink:
    def test_class_attributes(self):
        assert DataflowPipelineLink.key == "dataflow_pipeline_config"
        assert DataflowPipelineLink.name == "Dataflow Pipeline"
        assert DataflowPipelineLink.format_str == DATAFLOW_PIPELINE_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataflowPipelineLink.persist(
            context=mock_context,
            location=TEST_LOCATION,
            pipeline_name=TEST_PIPELINE_NAME,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="dataflow_pipeline_config",
            value={
                "location": TEST_LOCATION,
                "pipeline_name": TEST_PIPELINE_NAME,
                "project_id": TEST_PROJECT_ID,
            },
        )

    def test_format_link(self):
        link = DataflowPipelineLink()

        result = link._format_link(
            location=TEST_LOCATION, pipeline_name=TEST_PIPELINE_NAME, project_id=TEST_PROJECT_ID
        )

        assert result == BASE_LINK + DATAFLOW_PIPELINE_LINK.format(
            location=TEST_LOCATION, pipeline_name=TEST_PIPELINE_NAME, project_id=TEST_PROJECT_ID
        )
