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
from __future__ import annotations

from unittest import mock

from airflow.providers.google.marketing_platform.links.analytics_admin import (
    BASE_LINK,
    GoogleAnalyticsPropertyLink,
)

TEST_PROPERTY_ID = "123456789"
TEST_PROJECT_ID = "test_project"
TEST_CONF_GOOGLE_ADS_LINK = {"property_id": TEST_PROJECT_ID}
ANALYTICS_LINKS_PATH = "airflow.providers.google.marketing_platform.links.analytics_admin"


class TestGoogleAnalyticsPropertyLink:
    @mock.patch(f"{ANALYTICS_LINKS_PATH}.XCom")
    def test_get_link(self, mock_xcom):
        mock_ti_key = mock.MagicMock()
        mock_xcom.get_value.return_value = TEST_CONF_GOOGLE_ADS_LINK
        url_expected = f"{BASE_LINK}#/p{TEST_PROJECT_ID}/"

        link = GoogleAnalyticsPropertyLink()
        url = link.get_link(operator=mock.MagicMock(), ti_key=mock_ti_key)

        mock_xcom.get_value.assert_called_once_with(key=link.key, ti_key=mock_ti_key)
        assert url == url_expected

    @mock.patch(f"{ANALYTICS_LINKS_PATH}.XCom")
    def test_get_link_not_found(self, mock_xcom):
        mock_ti_key = mock.MagicMock()
        mock_xcom.get_value.return_value = None

        link = GoogleAnalyticsPropertyLink()
        url = link.get_link(operator=mock.MagicMock(), ti_key=mock_ti_key)

        mock_xcom.get_value.assert_called_once_with(key=link.key, ti_key=mock_ti_key)
        assert url == ""

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_task_instance = mock.MagicMock()

        GoogleAnalyticsPropertyLink.persist(
            context=mock_context,
            task_instance=mock_task_instance,
            property_id=TEST_PROPERTY_ID,
        )

        mock_task_instance.xcom_push.assert_called_once_with(
            mock_context,
            key=GoogleAnalyticsPropertyLink.key,
            value={"property_id": TEST_PROPERTY_ID},
        )
