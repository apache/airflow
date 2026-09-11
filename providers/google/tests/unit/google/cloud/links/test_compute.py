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

"""Tests for Compute Engine links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.compute import (
    COMPUTE_GROUP_MANAGER_LINK,
    COMPUTE_LINK,
    COMPUTE_TEMPLATE_LINK,
    ComputeInstanceDetailsLink,
    ComputeInstanceGroupManagerDetailsLink,
    ComputeInstanceTemplateDetailsLink,
)

TEST_LOCATION_ID = "test-location-id"
TEST_PROJECT_ID = "test-project-id"
TEST_RESOURCE_ID = "test-resource-id"


class TestComputeInstanceDetailsLink:
    def test_class_attributes(self):
        assert ComputeInstanceDetailsLink.key == "compute_instance_details"
        assert ComputeInstanceDetailsLink.name == "Compute Instance details"
        assert ComputeInstanceDetailsLink.format_str == COMPUTE_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        ComputeInstanceDetailsLink.persist(
            context=mock_context,
            location_id=TEST_LOCATION_ID,
            project_id=TEST_PROJECT_ID,
            resource_id=TEST_RESOURCE_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="compute_instance_details",
            value={
                "location_id": TEST_LOCATION_ID,
                "project_id": TEST_PROJECT_ID,
                "resource_id": TEST_RESOURCE_ID,
            },
        )

    def test_format_link(self):
        link = ComputeInstanceDetailsLink()

        result = link._format_link(
            location_id=TEST_LOCATION_ID, project_id=TEST_PROJECT_ID, resource_id=TEST_RESOURCE_ID
        )

        # ``COMPUTE_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == COMPUTE_LINK.format(
            location_id=TEST_LOCATION_ID, project_id=TEST_PROJECT_ID, resource_id=TEST_RESOURCE_ID
        )


class TestComputeInstanceTemplateDetailsLink:
    def test_class_attributes(self):
        assert ComputeInstanceTemplateDetailsLink.key == "compute_instance_template_details"
        assert ComputeInstanceTemplateDetailsLink.name == "Compute Instance Template details"
        assert ComputeInstanceTemplateDetailsLink.format_str == COMPUTE_TEMPLATE_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        ComputeInstanceTemplateDetailsLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            resource_id=TEST_RESOURCE_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="compute_instance_template_details",
            value={"project_id": TEST_PROJECT_ID, "resource_id": TEST_RESOURCE_ID},
        )

    def test_format_link(self):
        link = ComputeInstanceTemplateDetailsLink()

        result = link._format_link(project_id=TEST_PROJECT_ID, resource_id=TEST_RESOURCE_ID)

        # ``COMPUTE_TEMPLATE_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == COMPUTE_TEMPLATE_LINK.format(
            project_id=TEST_PROJECT_ID, resource_id=TEST_RESOURCE_ID
        )


class TestComputeInstanceGroupManagerDetailsLink:
    def test_class_attributes(self):
        assert ComputeInstanceGroupManagerDetailsLink.key == "compute_instance_group_manager_details"
        assert ComputeInstanceGroupManagerDetailsLink.name == "Compute Instance Group Manager"
        assert ComputeInstanceGroupManagerDetailsLink.format_str == COMPUTE_GROUP_MANAGER_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        ComputeInstanceGroupManagerDetailsLink.persist(
            context=mock_context,
            location_id=TEST_LOCATION_ID,
            project_id=TEST_PROJECT_ID,
            resource_id=TEST_RESOURCE_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="compute_instance_group_manager_details",
            value={
                "location_id": TEST_LOCATION_ID,
                "project_id": TEST_PROJECT_ID,
                "resource_id": TEST_RESOURCE_ID,
            },
        )

    def test_format_link(self):
        link = ComputeInstanceGroupManagerDetailsLink()

        result = link._format_link(
            location_id=TEST_LOCATION_ID, project_id=TEST_PROJECT_ID, resource_id=TEST_RESOURCE_ID
        )

        # ``COMPUTE_GROUP_MANAGER_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == COMPUTE_GROUP_MANAGER_LINK.format(
            location_id=TEST_LOCATION_ID, project_id=TEST_PROJECT_ID, resource_id=TEST_RESOURCE_ID
        )
