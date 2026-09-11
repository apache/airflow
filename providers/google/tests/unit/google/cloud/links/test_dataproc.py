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

"""Tests for Dataproc links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.dataproc import (
    DATAPROC_BATCH_LINK,
    DATAPROC_BATCHES_LINK,
    DATAPROC_CLUSTER_LINK,
    DATAPROC_JOB_LINK,
    DATAPROC_WORKFLOW_LINK,
    DATAPROC_WORKFLOW_TEMPLATE_LINK,
    DataprocBatchesListLink,
    DataprocBatchLink,
    DataprocClusterLink,
    DataprocJobLink,
    DataprocWorkflowLink,
    DataprocWorkflowTemplateLink,
)

TEST_BATCH_ID = "test-batch-id"
TEST_CLUSTER_ID = "test-cluster-id"
TEST_JOB_ID = "test-job-id"
TEST_PROJECT_ID = "test-project-id"
TEST_REGION = "test-region"
TEST_WORKFLOW_ID = "test-workflow-id"
TEST_WORKFLOW_TEMPLATE_ID = "test-workflow-template-id"


class TestDataprocClusterLink:
    def test_class_attributes(self):
        assert DataprocClusterLink.key == "dataproc_cluster"
        assert DataprocClusterLink.name == "Dataproc Cluster"
        assert DataprocClusterLink.format_str == DATAPROC_CLUSTER_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataprocClusterLink.persist(
            context=mock_context,
            cluster_id=TEST_CLUSTER_ID,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="dataproc_cluster",
            value={"cluster_id": TEST_CLUSTER_ID, "project_id": TEST_PROJECT_ID, "region": TEST_REGION},
        )

    def test_format_link(self):
        link = DataprocClusterLink()

        result = link._format_link(cluster_id=TEST_CLUSTER_ID, project_id=TEST_PROJECT_ID, region=TEST_REGION)

        # ``DATAPROC_CLUSTER_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DATAPROC_CLUSTER_LINK.format(
            cluster_id=TEST_CLUSTER_ID, project_id=TEST_PROJECT_ID, region=TEST_REGION
        )


class TestDataprocJobLink:
    def test_class_attributes(self):
        assert DataprocJobLink.key == "dataproc_job"
        assert DataprocJobLink.name == "Dataproc Job"
        assert DataprocJobLink.format_str == DATAPROC_JOB_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataprocJobLink.persist(
            context=mock_context,
            job_id=TEST_JOB_ID,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="dataproc_job",
            value={"job_id": TEST_JOB_ID, "project_id": TEST_PROJECT_ID, "region": TEST_REGION},
        )

    def test_format_link(self):
        link = DataprocJobLink()

        result = link._format_link(job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, region=TEST_REGION)

        # ``DATAPROC_JOB_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DATAPROC_JOB_LINK.format(
            job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, region=TEST_REGION
        )


class TestDataprocWorkflowLink:
    def test_class_attributes(self):
        assert DataprocWorkflowLink.key == "dataproc_workflow"
        assert DataprocWorkflowLink.name == "Dataproc Workflow"
        assert DataprocWorkflowLink.format_str == DATAPROC_WORKFLOW_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataprocWorkflowLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            workflow_id=TEST_WORKFLOW_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="dataproc_workflow",
            value={"project_id": TEST_PROJECT_ID, "region": TEST_REGION, "workflow_id": TEST_WORKFLOW_ID},
        )

    def test_format_link(self):
        link = DataprocWorkflowLink()

        result = link._format_link(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, workflow_id=TEST_WORKFLOW_ID
        )

        # ``DATAPROC_WORKFLOW_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DATAPROC_WORKFLOW_LINK.format(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, workflow_id=TEST_WORKFLOW_ID
        )


class TestDataprocWorkflowTemplateLink:
    def test_class_attributes(self):
        assert DataprocWorkflowTemplateLink.key == "dataproc_workflow_template"
        assert DataprocWorkflowTemplateLink.name == "Dataproc Workflow Template"
        assert DataprocWorkflowTemplateLink.format_str == DATAPROC_WORKFLOW_TEMPLATE_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataprocWorkflowTemplateLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            workflow_template_id=TEST_WORKFLOW_TEMPLATE_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="dataproc_workflow_template",
            value={
                "project_id": TEST_PROJECT_ID,
                "region": TEST_REGION,
                "workflow_template_id": TEST_WORKFLOW_TEMPLATE_ID,
            },
        )

    def test_format_link(self):
        link = DataprocWorkflowTemplateLink()

        result = link._format_link(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, workflow_template_id=TEST_WORKFLOW_TEMPLATE_ID
        )

        # ``DATAPROC_WORKFLOW_TEMPLATE_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DATAPROC_WORKFLOW_TEMPLATE_LINK.format(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, workflow_template_id=TEST_WORKFLOW_TEMPLATE_ID
        )


class TestDataprocBatchLink:
    def test_class_attributes(self):
        assert DataprocBatchLink.key == "dataproc_batch"
        assert DataprocBatchLink.name == "Dataproc Batch"
        assert DataprocBatchLink.format_str == DATAPROC_BATCH_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataprocBatchLink.persist(
            context=mock_context,
            batch_id=TEST_BATCH_ID,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="dataproc_batch",
            value={"batch_id": TEST_BATCH_ID, "project_id": TEST_PROJECT_ID, "region": TEST_REGION},
        )

    def test_format_link(self):
        link = DataprocBatchLink()

        result = link._format_link(batch_id=TEST_BATCH_ID, project_id=TEST_PROJECT_ID, region=TEST_REGION)

        # ``DATAPROC_BATCH_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DATAPROC_BATCH_LINK.format(
            batch_id=TEST_BATCH_ID, project_id=TEST_PROJECT_ID, region=TEST_REGION
        )


class TestDataprocBatchesListLink:
    def test_class_attributes(self):
        assert DataprocBatchesListLink.key == "dataproc_batches_list"
        assert DataprocBatchesListLink.name == "Dataproc Batches List"
        assert DataprocBatchesListLink.format_str == DATAPROC_BATCHES_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataprocBatchesListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="dataproc_batches_list",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = DataprocBatchesListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        # ``DATAPROC_BATCHES_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == DATAPROC_BATCHES_LINK.format(project_id=TEST_PROJECT_ID)
