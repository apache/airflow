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

from unittest.mock import MagicMock

import pytest

from airflow.sdk.bases.operatorlink import BaseOperatorLink


class StaticLink(BaseOperatorLink):
    """Link that uses static_url — no XCom needed."""

    name = "static"
    static_url = "https://example.com/docs"


class DynamicLink(BaseOperatorLink):
    """Link that overrides get_link the traditional XCom way."""

    name = "dynamic"

    def get_link(self, operator, *, ti_key):
        return "https://example.com/runs/123"


class EmptyLink(BaseOperatorLink):
    """Link that overrides neither static_url nor get_link — should raise."""

    name = "empty"


class TestBaseOperatorLinkDefaults:
    def test_static_url_defaults_to_none(self):
        """Base class static_url returns None by default."""
        link = DynamicLink()
        assert link.static_url is None

    def test_xcom_key_uses_class_name(self):
        link = StaticLink()
        assert link.xcom_key == "_link_StaticLink"

    def test_xcom_key_uses_subclass_name(self):
        link = DynamicLink()
        assert link.xcom_key == "_link_DynamicLink"


class TestStaticUrl:
    def test_get_link_returns_static_url(self):
        """get_link() should return static_url when it is set."""
        link = StaticLink()
        result = link.get_link(operator=MagicMock(), ti_key=MagicMock())
        assert result == "https://example.com/docs"

    def test_static_url_value(self):
        link = StaticLink()
        assert link.static_url == "https://example.com/docs"

    def test_no_xcom_push_needed(self):
        """Calling get_link on a static link requires no operator or ti_key data."""
        link = StaticLink()
        # Both args are irrelevant — MagicMock with no setup is sufficient
        result = link.get_link(operator=MagicMock(), ti_key=MagicMock())
        assert result == "https://example.com/docs"


class TestDynamicLink:
    def test_get_link_traditional_override_works(self):
        """Subclasses that override get_link directly still work as before."""
        link = DynamicLink()
        result = link.get_link(operator=MagicMock(), ti_key=MagicMock())
        assert result == "https://example.com/runs/123"

    def test_static_url_is_none_for_dynamic_link(self):
        link = DynamicLink()
        assert link.static_url is None


class TestEmptyLink:
    def test_get_link_raises_not_implemented(self):
        """A subclass that overrides neither static_url nor get_link raises NotImplementedError."""
        link = EmptyLink()
        with pytest.raises(NotImplementedError, match="EmptyLink"):
            link.get_link(operator=MagicMock(), ti_key=MagicMock())

    def test_error_message_mentions_both_options(self):
        """The error should guide the developer toward both override options."""
        link = EmptyLink()
        with pytest.raises(NotImplementedError, match="static_url"):
            link.get_link(operator=MagicMock(), ti_key=MagicMock())


class TestNameIsAbstract:
    def test_cannot_instantiate_without_name(self):
        """BaseOperatorLink.name is abstract — subclasses must define it."""
        with pytest.raises(TypeError, match="name"):

            class NoNameLink(BaseOperatorLink):
                pass

            NoNameLink()