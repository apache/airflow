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

"""Tests for Stackdriver links."""

from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.links.base import BASE_LINK
from airflow.providers.google.cloud.links.stackdriver import (
    STACKDRIVER_NOTIFICATIONS_LINK,
    STACKDRIVER_POLICIES_LINK,
    StackdriverNotificationsLink,
    StackdriverPoliciesLink,
)

TEST_PROJECT_ID = "test-project-id"


class TestStackdriverNotificationsLink:
    def test_class_attributes(self):
        assert StackdriverNotificationsLink.key == "stackdriver_notifications"
        assert StackdriverNotificationsLink.name == "Cloud Monitoring Notifications"
        assert StackdriverNotificationsLink.format_str == STACKDRIVER_NOTIFICATIONS_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        StackdriverNotificationsLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="stackdriver_notifications",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = StackdriverNotificationsLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        assert result == BASE_LINK + STACKDRIVER_NOTIFICATIONS_LINK.format(project_id=TEST_PROJECT_ID)


class TestStackdriverPoliciesLink:
    def test_class_attributes(self):
        assert StackdriverPoliciesLink.key == "stackdriver_policies"
        assert StackdriverPoliciesLink.name == "Cloud Monitoring Policies"
        assert StackdriverPoliciesLink.format_str == STACKDRIVER_POLICIES_LINK

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])

        StackdriverPoliciesLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key="stackdriver_policies",
            value={"project_id": TEST_PROJECT_ID},
        )

    def test_format_link(self):
        link = StackdriverPoliciesLink()

        result = link._format_link(project_id=TEST_PROJECT_ID)

        assert result == BASE_LINK + STACKDRIVER_POLICIES_LINK.format(project_id=TEST_PROJECT_ID)
