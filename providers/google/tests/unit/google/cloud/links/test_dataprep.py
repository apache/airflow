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
"""Tests for Dataprep links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.dataprep import (
    DATAPREP_FLOW_LINK,
    DATAPREP_JOB_GROUP_LINK,
    DataprepFlowLink,
    DataprepJobGroupLink,
)

TEST_PROJECT_ID = "test-project"
TEST_FLOW_ID = 1234
TEST_JOB_GROUP_ID = 5678


class TestDataprepFlowLink:
    def test_class_attributes(self):
        assert DataprepFlowLink.key == "dataprep_flow_page"
        assert DataprepFlowLink.name == "Flow details page"
        assert DataprepFlowLink.format_str == DATAPREP_FLOW_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataprepFlowLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            flow_id=TEST_FLOW_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="dataprep_flow_page",
            value={"project_id": TEST_PROJECT_ID, "flow_id": TEST_FLOW_ID},
        )

    def test_format_link_is_absolute(self):
        link = DataprepFlowLink()
        result = link._format_link(project_id=TEST_PROJECT_ID, flow_id=TEST_FLOW_ID)
        # Dataprep links are already absolute, so `_format_link` must not prepend
        # the Google Cloud console BASE_LINK.
        assert result == DATAPREP_FLOW_LINK.format(project_id=TEST_PROJECT_ID, flow_id=TEST_FLOW_ID)
        assert result.startswith("https://clouddataprep.com")


class TestDataprepJobGroupLink:
    def test_class_attributes(self):
        assert DataprepJobGroupLink.key == "dataprep_job_group_page"
        assert DataprepJobGroupLink.name == "Job group details page"
        assert DataprepJobGroupLink.format_str == DATAPREP_JOB_GROUP_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        DataprepJobGroupLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            job_group_id=TEST_JOB_GROUP_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="dataprep_job_group_page",
            value={"project_id": TEST_PROJECT_ID, "job_group_id": TEST_JOB_GROUP_ID},
        )

    def test_format_link_is_absolute(self):
        link = DataprepJobGroupLink()
        result = link._format_link(project_id=TEST_PROJECT_ID, job_group_id=TEST_JOB_GROUP_ID)
        assert result == DATAPREP_JOB_GROUP_LINK.format(
            project_id=TEST_PROJECT_ID, job_group_id=TEST_JOB_GROUP_ID
        )
        assert result.startswith("https://clouddataprep.com")
