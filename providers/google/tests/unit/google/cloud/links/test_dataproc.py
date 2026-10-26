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
"""Tests for Google Cloud Dataproc links.

Expected URLs are fully hard-coded: the path and query parts are never derived from the
constants under test, so a broken or hand-edited ``format_str`` always fails the test. The
``key`` values are asserted as literals too - they are stored in XComs and serialized DAGs,
so values that merely mirror the module constants would not catch a rename.
"""

from __future__ import annotations

from unittest import mock

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.links import dataproc
from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.dataproc import (
    DataprocBatchesListLink,
    DataprocBatchLink,
    DataprocClusterLink,
    DataprocJobLink,
    DataprocLink,
    DataprocListLink,
    DataprocWorkflowLink,
    DataprocWorkflowTemplateLink,
)

TEST_PROJECT_ID = "test-project"
TEST_REGION = "us-central1"

# The legacy links store the whole URL template in the XCom, not just the identifiers.
LEGACY_JOB_URL_TEMPLATE = (
    "https://console.cloud.google.com/dataproc/jobs/{resource}?region={region}&project={project_id}"
)
LEGACY_BATCHES_URL_TEMPLATE = "https://console.cloud.google.com/dataproc/batches?project={project_id}"


def _legacy_link_instance_with_warning():
    with pytest.warns(AirflowProviderDeprecationWarning):
        return DataprocListLink()


class TestDataprocLink:
    def test_class_attributes(self):
        assert DataprocLink.key == "conf"
        assert DataprocLink.name == "Dataproc resource"

    def test_persist(self):
        mock_context = mock.MagicMock()

        DataprocLink.persist(
            context=mock_context,
            url=LEGACY_JOB_URL_TEMPLATE,
            resource="test-job-1",
            region=TEST_REGION,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["task_instance"].xcom_push.assert_called_once_with(
            key="conf",
            value={
                "region": TEST_REGION,
                "project_id": TEST_PROJECT_ID,
                "url": LEGACY_JOB_URL_TEMPLATE,
                "resource": "test-job-1",
            },
        )

    def test_get_link_formats_the_xcom_url(self):
        link = DataprocLink()
        conf = {
            "url": LEGACY_JOB_URL_TEMPLATE,
            "region": TEST_REGION,
            "project_id": TEST_PROJECT_ID,
            "resource": "test-job-1",
        }

        with mock.patch("airflow.providers.google.cloud.links.dataproc.XCom.get_value", return_value=conf):
            result = link.get_link(operator=mock.MagicMock(), ti_key=mock.MagicMock())

        assert (
            result
            == "https://console.cloud.google.com/dataproc/jobs/test-job-1?region=us-central1&project=test-project"
        )

    def test_get_link_returns_empty_string_when_xcom_missing(self):
        link = DataprocLink()

        with mock.patch("airflow.providers.google.cloud.links.dataproc.XCom.get_value", return_value=None):
            result = link.get_link(operator=mock.MagicMock(), ti_key=mock.MagicMock())

        assert result == ""


class TestDataprocListLink:
    def test_instantiation_warns_about_deprecation(self):
        with pytest.warns(AirflowProviderDeprecationWarning):
            DataprocListLink()

    def test_class_attributes(self):
        link = _legacy_link_instance_with_warning()
        assert link.key == "list_conf"
        assert link.name == "Dataproc resources"

    def test_persist(self):
        mock_context = mock.MagicMock()

        DataprocListLink.persist(
            context=mock_context,
            url=LEGACY_BATCHES_URL_TEMPLATE,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["task_instance"].xcom_push.assert_called_once_with(
            key="list_conf",
            value={"project_id": TEST_PROJECT_ID, "url": LEGACY_BATCHES_URL_TEMPLATE},
        )

    def test_get_link_formats_the_xcom_url(self):
        link = _legacy_link_instance_with_warning()
        conf = {"url": LEGACY_BATCHES_URL_TEMPLATE, "project_id": TEST_PROJECT_ID}

        with mock.patch("airflow.providers.google.cloud.links.dataproc.XCom.get_value", return_value=conf):
            result = link.get_link(operator=mock.MagicMock(), ti_key=mock.MagicMock())

        assert result == "https://console.cloud.google.com/dataproc/batches?project=test-project"


class TestDataprocClusterLink:
    def test_class_attributes(self):
        assert DataprocClusterLink.key == "dataproc_cluster"
        assert DataprocClusterLink.name == "Dataproc Cluster"

    def test_get_link(self):
        result = DataprocClusterLink()._format_link(
            cluster_id="test-cluster", region=TEST_REGION, project_id=TEST_PROJECT_ID
        )
        assert result == (
            BASE_LINK + "/dataproc/clusters/test-cluster/monitoring?region=us-central1&project=test-project"
        )


class TestDataprocJobLink:
    def test_class_attributes(self):
        assert DataprocJobLink.key == "dataproc_job"
        assert DataprocJobLink.name == "Dataproc Job"

    def test_get_link(self):
        result = DataprocJobLink()._format_link(
            job_id="test-job-1", region=TEST_REGION, project_id=TEST_PROJECT_ID
        )
        assert result == BASE_LINK + "/dataproc/jobs/test-job-1?region=us-central1&project=test-project"


class TestDataprocWorkflowLink:
    def test_class_attributes(self):
        assert DataprocWorkflowLink.key == "dataproc_workflow"
        assert DataprocWorkflowLink.name == "Dataproc Workflow"

    def test_get_link(self):
        result = DataprocWorkflowLink()._format_link(
            region=TEST_REGION, workflow_id="test-workflow-1", project_id=TEST_PROJECT_ID
        )
        assert result == (
            BASE_LINK + "/dataproc/workflows/instances/us-central1/test-workflow-1?project=test-project"
        )


class TestDataprocWorkflowTemplateLink:
    def test_class_attributes(self):
        assert DataprocWorkflowTemplateLink.key == "dataproc_workflow_template"
        assert DataprocWorkflowTemplateLink.name == "Dataproc Workflow Template"

    def test_get_link(self):
        result = DataprocWorkflowTemplateLink()._format_link(
            region=TEST_REGION, workflow_template_id="test-template-1", project_id=TEST_PROJECT_ID
        )
        assert result == (
            BASE_LINK + "/dataproc/workflows/templates/us-central1/test-template-1?project=test-project"
        )


class TestDataprocBatchLink:
    def test_class_attributes(self):
        assert DataprocBatchLink.key == "dataproc_batch"
        assert DataprocBatchLink.name == "Dataproc Batch"

    def test_get_link(self):
        # Note the template orders the path elements as "region, batch", not "batch, region".
        result = DataprocBatchLink()._format_link(
            region=TEST_REGION, batch_id="test-batch-1", project_id=TEST_PROJECT_ID
        )
        assert result == (
            BASE_LINK + "/dataproc/batches/us-central1/test-batch-1/monitoring?project=test-project"
        )


class TestDataprocBatchesListLink:
    def test_class_attributes(self):
        assert DataprocBatchesListLink.key == "dataproc_batches_list"
        assert DataprocBatchesListLink.name == "Dataproc Batches List"

    def test_get_link(self):
        result = DataprocBatchesListLink()._format_link(project_id=TEST_PROJECT_ID)
        assert result == BASE_LINK + "/dataproc/batches?project=test-project"


class TestDeprecatedModuleAlias:
    def test_legacy_alias_warns_and_resolves_to_new_constant(self):
        with pytest.warns(AirflowProviderDeprecationWarning):
            alias = getattr(dataproc, "DATAPROC_JOB_LOG_LINK")
        assert alias == dataproc.DATAPROC_JOB_LINK

    def test_unknown_attribute_raises(self):
        with pytest.raises(AttributeError):
            getattr(dataproc, "DATAPROC_UNKNOWN_LINK")
