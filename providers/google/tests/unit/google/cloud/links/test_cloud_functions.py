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

"""Tests for Cloud Functions links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.cloud_functions import (
    CLOUD_FUNCTIONS_DETAILS_LINK,
    CLOUD_FUNCTIONS_LIST_LINK,
    CloudFunctionsDetailsLink,
    CloudFunctionsListLink,
)

TEST_FUNCTION_NAME = "test-function-name"
TEST_LOCATION = "test-location"
TEST_PROJECT_ID = "test-project-id"


class TestCloudFunctionsDetailsLink:
    def test_class_attributes(self):
        assert CloudFunctionsDetailsLink.key == "cloud_functions_details"
        assert CloudFunctionsDetailsLink.name == "Cloud Functions Details"
        assert CloudFunctionsDetailsLink.format_str == CLOUD_FUNCTIONS_DETAILS_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudFunctionsDetailsLink.persist(
            context=mock_context,
            function_name=TEST_FUNCTION_NAME,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_functions_details",
            value={
                "function_name": TEST_FUNCTION_NAME,
                "location": TEST_LOCATION,
                "project_id": TEST_PROJECT_ID,
            },
        )

    def test_format_link(self):
        link = CloudFunctionsDetailsLink()

        result = link._format_link(
            function_name=TEST_FUNCTION_NAME, location=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )

        # ``CLOUD_FUNCTIONS_DETAILS_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == CLOUD_FUNCTIONS_DETAILS_LINK.format(
            function_name=TEST_FUNCTION_NAME, location=TEST_LOCATION, project_id=TEST_PROJECT_ID
        )


class TestCloudFunctionsListLink:
    def test_class_attributes(self):
        assert CloudFunctionsListLink.key == "cloud_functions_list"
        assert CloudFunctionsListLink.name == "Cloud Functions List"
        assert CloudFunctionsListLink.format_str == CLOUD_FUNCTIONS_LIST_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        CloudFunctionsListLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="cloud_functions_list",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = CloudFunctionsListLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        # ``CLOUD_FUNCTIONS_LIST_LINK`` already embeds ``BASE_LINK``, so ``_format_link``
        # returns the formatted string instead of prefixing it a second time.
        assert result == CLOUD_FUNCTIONS_LIST_LINK.format(project_id=TEST_PROJECT_ID)
