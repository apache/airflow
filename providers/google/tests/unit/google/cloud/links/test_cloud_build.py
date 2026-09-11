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

"""Tests for Cloud Build links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.cloud_build import (
    BUILD_LINK,
    BUILD_LIST_LINK,
    BUILD_TRIGGER_DETAILS_LINK,
    BUILD_TRIGGERS_LIST_LINK,
    CloudBuildLink,
    CloudBuildListLink,
    CloudBuildTriggerDetailsLink,
    CloudBuildTriggersListLink,
)

TEST_BUILD_ID = "test-build-id"
TEST_PROJECT_ID = "test-project-id"
TEST_REGION = "test-region"
TEST_TRIGGER_ID = "test-trigger-id"


class TestCloudBuildLink:
    def test_class_attributes(self):
        assert CloudBuildLink.key == "cloud_build_key"
        assert CloudBuildLink.name == "Cloud Build Details"
        assert CloudBuildLink.format_str == BUILD_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudBuildLink.persist(
            context=mock_context,
            build_id=TEST_BUILD_ID,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_build_key",
            value={"build_id": TEST_BUILD_ID, "project_id": TEST_PROJECT_ID, "region": TEST_REGION},
        )

    def test_format_link(self):
        link = CloudBuildLink()

        result = link._format_link(build_id=TEST_BUILD_ID, project_id=TEST_PROJECT_ID, region=TEST_REGION)

        assert result == BASE_LINK + BUILD_LINK.format(
            build_id=TEST_BUILD_ID, project_id=TEST_PROJECT_ID, region=TEST_REGION
        )


class TestCloudBuildListLink:
    def test_class_attributes(self):
        assert CloudBuildListLink.key == "cloud_build_list_key"
        assert CloudBuildListLink.name == "Cloud Builds List"
        assert CloudBuildListLink.format_str == BUILD_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudBuildListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_build_list_key",
            value={"project_id": TEST_PROJECT_ID, "region": TEST_REGION},
        )

    def test_format_link(self):
        link = CloudBuildListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID, region=TEST_REGION)

        assert result == BASE_LINK + BUILD_LIST_LINK.format(project_id=TEST_PROJECT_ID, region=TEST_REGION)


class TestCloudBuildTriggersListLink:
    def test_class_attributes(self):
        assert CloudBuildTriggersListLink.key == "cloud_build_triggers_list_key"
        assert CloudBuildTriggersListLink.name == "Cloud Build Triggers List"
        assert CloudBuildTriggersListLink.format_str == BUILD_TRIGGERS_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudBuildTriggersListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_build_triggers_list_key",
            value={"project_id": TEST_PROJECT_ID, "region": TEST_REGION},
        )

    def test_format_link(self):
        link = CloudBuildTriggersListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID, region=TEST_REGION)

        assert result == BASE_LINK + BUILD_TRIGGERS_LIST_LINK.format(
            project_id=TEST_PROJECT_ID, region=TEST_REGION
        )


class TestCloudBuildTriggerDetailsLink:
    def test_class_attributes(self):
        assert CloudBuildTriggerDetailsLink.key == "cloud_build_triggers_details_key"
        assert CloudBuildTriggerDetailsLink.name == "Cloud Build Triggers Details"
        assert CloudBuildTriggerDetailsLink.format_str == BUILD_TRIGGER_DETAILS_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudBuildTriggerDetailsLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            trigger_id=TEST_TRIGGER_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_build_triggers_details_key",
            value={"project_id": TEST_PROJECT_ID, "region": TEST_REGION, "trigger_id": TEST_TRIGGER_ID},
        )

    def test_format_link(self):
        link = CloudBuildTriggerDetailsLink()

        result = link._format_link(project_id=TEST_PROJECT_ID, region=TEST_REGION, trigger_id=TEST_TRIGGER_ID)

        assert result == BASE_LINK + BUILD_TRIGGER_DETAILS_LINK.format(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, trigger_id=TEST_TRIGGER_ID
        )
