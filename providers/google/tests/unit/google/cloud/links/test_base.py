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

from airflow.providers.google.cloud.links.base import BASE_LINK, BaseGoogleLink

# ---------------------------------------------------------------------------
# Concrete subclass used throughout the tests
# ---------------------------------------------------------------------------
TEST_KEY = "test_link"
TEST_NAME = "Test Link"
TEST_FORMAT_STR = "/test/{project_id}/{resource_id}"
TEST_PROJECT_ID = "test-project"
TEST_RESOURCE_ID = "test-resource"


class ConcreteGoogleLink(BaseGoogleLink):
    key = TEST_KEY
    name = TEST_NAME
    format_str = TEST_FORMAT_STR


class TestBaseGoogleLinkXcomKey:
    def test_xcom_key_equals_key(self):
        link = ConcreteGoogleLink()
        assert link.xcom_key == TEST_KEY


class TestBaseGoogleLinkPersist:
    def test_persist_pushes_to_xcom(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock(spec=[])  # no extra_links_params

        ConcreteGoogleLink.persist(
            context=mock_context,
            project_id=TEST_PROJECT_ID,
            resource_id=TEST_RESOURCE_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key=TEST_KEY,
            value={"project_id": TEST_PROJECT_ID, "resource_id": TEST_RESOURCE_ID},
        )


class TestBaseGoogleLinkGetConfig:
    def test_returns_none_when_no_config(self):
        link = ConcreteGoogleLink()
        operator = mock.MagicMock(spec=[])  # no extra_links_params
        ti_key = mock.MagicMock()

        with mock.patch("airflow.providers.google.cloud.links.base.XCom.get_value", return_value=None):
            result = link.get_config(operator, ti_key)

        assert result is None

    def test_returns_merged_config(self):
        link = ConcreteGoogleLink()
        operator = mock.MagicMock(extra_links_params={"project_id": "from-operator"})
        ti_key = mock.MagicMock()

        with mock.patch(
            "airflow.providers.google.cloud.links.base.XCom.get_value",
            return_value={"resource_id": "from-xcom"},
        ):
            result = link.get_config(operator, ti_key)

        assert result == {
            "project_id": "from-operator",
            "resource_id": "from-xcom",
            "namespace": "default",  # default injected for datafusion back-compat
        }

    def test_xcom_overrides_operator_params(self):
        link = ConcreteGoogleLink()
        operator = mock.MagicMock(extra_links_params={"project_id": "from-operator"})
        ti_key = mock.MagicMock()

        with mock.patch(
            "airflow.providers.google.cloud.links.base.XCom.get_value",
            return_value={"project_id": "from-xcom", "resource_id": TEST_RESOURCE_ID},
        ):
            result = link.get_config(operator, ti_key)

        assert result["project_id"] == "from-xcom"


class TestBaseGoogleLinkFormatLink:
    def test_formats_relative_path_with_base_link(self):
        link = ConcreteGoogleLink()
        result = link._format_link(project_id=TEST_PROJECT_ID, resource_id=TEST_RESOURCE_ID)
        assert result == BASE_LINK + f"/test/{TEST_PROJECT_ID}/{TEST_RESOURCE_ID}"

    def test_returns_empty_string_on_missing_key(self):
        link = ConcreteGoogleLink()
        result = link._format_link(project_id=TEST_PROJECT_ID)  # missing resource_id
        assert result == ""

    def test_returns_absolute_url_unchanged(self):
        class AbsoluteLink(BaseGoogleLink):
            key = "abs_link"
            name = "Abs Link"
            format_str = "https://example.com/{resource_id}"

        link = AbsoluteLink()
        result = link._format_link(resource_id="res")
        assert result == "https://example.com/res"


class TestBaseGoogleLinkGetLink:
    def test_returns_empty_string_when_no_config(self):
        link = ConcreteGoogleLink()
        operator = mock.MagicMock(spec=[], extra_links_params={})
        ti_key = mock.MagicMock()

        with mock.patch("airflow.providers.google.cloud.links.base.XCom.get_value", return_value=None):
            result = link.get_link(operator=operator, ti_key=ti_key)

        assert result == ""

    def test_returns_http_string_directly_without_formatting(self):
        """If XCom already holds a full http URL it should be returned as-is."""
        link = ConcreteGoogleLink()
        operator = mock.MagicMock(spec=[])
        ti_key = mock.MagicMock()
        stored_url = "https://console.cloud.google.com/already/formatted"

        with mock.patch(
            "airflow.providers.google.cloud.links.base.XCom.get_value",
            return_value=stored_url,
        ):
            result = link.get_link(operator=operator, ti_key=ti_key)

        assert result == stored_url

    def test_formats_link_from_config(self):
        link = ConcreteGoogleLink()
        operator = mock.MagicMock(spec=[], extra_links_params={})
        ti_key = mock.MagicMock()
        xcom_value = {"project_id": TEST_PROJECT_ID, "resource_id": TEST_RESOURCE_ID}

        with mock.patch(
            "airflow.providers.google.cloud.links.base.XCom.get_value",
            return_value=xcom_value,
        ):
            result = link.get_link(operator=operator, ti_key=ti_key)

        expected = BASE_LINK + f"/test/{TEST_PROJECT_ID}/{TEST_RESOURCE_ID}"
        assert result == expected
