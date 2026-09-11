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

"""Tests for Data Fusion links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.datafusion import (
    DATAFUSION_INSTANCE_LINK,
    DATAFUSION_PIPELINE_LINK,
    DATAFUSION_PIPELINES_LINK,
    DataFusionInstanceLink,
    DataFusionPipelineLink,
    DataFusionPipelinesLink,
)

TEST_INSTANCE_NAME = "test-instance-name"
TEST_NAMESPACE = "test-namespace"
TEST_PIPELINE_NAME = "test-pipeline-name"
TEST_PROJECT_ID = "test-project-id"
TEST_REGION = "test-region"
TEST_URI = "https://test-uri"


class TestDataFusionInstanceLink:
    def test_class_attributes(self):
        assert DataFusionInstanceLink.key == "instance_conf"
        assert DataFusionInstanceLink.name == "Data Fusion Instance"
        assert DataFusionInstanceLink.format_str == DATAFUSION_INSTANCE_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataFusionInstanceLink.persist(
            context=mock_context,
            instance_name=TEST_INSTANCE_NAME,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="instance_conf",
            value={"instance_name": TEST_INSTANCE_NAME, "project_id": TEST_PROJECT_ID, "region": TEST_REGION},
        )

    def test_format_link(self):
        link = DataFusionInstanceLink()

        result = link._format_link(
            instance_name=TEST_INSTANCE_NAME, project_id=TEST_PROJECT_ID, region=TEST_REGION
        )

        # ``DATAFUSION_INSTANCE_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DATAFUSION_INSTANCE_LINK.format(
            instance_name=TEST_INSTANCE_NAME, project_id=TEST_PROJECT_ID, region=TEST_REGION
        )


class TestDataFusionPipelineLink:
    def test_class_attributes(self):
        assert DataFusionPipelineLink.key == "pipeline_conf"
        assert DataFusionPipelineLink.name == "Data Fusion Pipeline"
        assert DataFusionPipelineLink.format_str == DATAFUSION_PIPELINE_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataFusionPipelineLink.persist(
            context=mock_context,
            namespace=TEST_NAMESPACE,
            pipeline_name=TEST_PIPELINE_NAME,
            uri=TEST_URI,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="pipeline_conf",
            value={"namespace": TEST_NAMESPACE, "pipeline_name": TEST_PIPELINE_NAME, "uri": TEST_URI},
        )

    def test_format_link(self):
        link = DataFusionPipelineLink()

        result = link._format_link(namespace=TEST_NAMESPACE, pipeline_name=TEST_PIPELINE_NAME, uri=TEST_URI)

        # The formatted URL is already absolute and points outside the Google Cloud
        # console, so ``_format_link`` returns it as-is.
        assert result == DATAFUSION_PIPELINE_LINK.format(
            namespace=TEST_NAMESPACE, pipeline_name=TEST_PIPELINE_NAME, uri=TEST_URI
        )


class TestDataFusionPipelinesLink:
    def test_class_attributes(self):
        assert DataFusionPipelinesLink.key == "pipelines_conf"
        assert DataFusionPipelinesLink.name == "Data Fusion Pipelines List"
        assert DataFusionPipelinesLink.format_str == DATAFUSION_PIPELINES_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataFusionPipelinesLink.persist(
            context=mock_context,
            namespace=TEST_NAMESPACE,
            uri=TEST_URI,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="pipelines_conf",
            value={"namespace": TEST_NAMESPACE, "uri": TEST_URI},
        )

    def test_format_link(self):
        link = DataFusionPipelinesLink()

        result = link._format_link(namespace=TEST_NAMESPACE, uri=TEST_URI)

        # The formatted URL is already absolute and points outside the Google Cloud
        # console, so ``_format_link`` returns it as-is.
        assert result == DATAFUSION_PIPELINES_LINK.format(namespace=TEST_NAMESPACE, uri=TEST_URI)
