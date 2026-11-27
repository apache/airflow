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

import pytest

from airflow.sdk.bases.operatorlink import BaseOperatorLink


class DynamicLink(BaseOperatorLink):
    """A conventional dynamic link that overrides ``get_link``."""

    @property
    def name(self) -> str:
        return "dynamic"

    def get_link(self, operator, *, ti_key) -> str:
        return "https://example.com/dynamic"


class StaticLink(BaseOperatorLink):
    """A link that overrides only ``static_url`` (the shape this PR enables)."""

    @property
    def name(self) -> str:
        return "static"

    @property
    def static_url(self) -> str | None:
        return "https://example.com/static"


class DynamicIntermediate(BaseOperatorLink):
    """An intermediate base that defines one of the two link sources."""

    def get_link(self, operator, *, ti_key) -> str:
        return "https://example.com/intermediate"


class ConcreteThroughIntermediate(DynamicIntermediate):
    @property
    def name(self) -> str:
        return "through-intermediate"


def test_dynamic_link_is_accepted_at_definition():
    """A subclass overriding ``get_link`` defines cleanly."""
    assert DynamicLink().get_link(None, ti_key=None) == "https://example.com/dynamic"


def test_static_url_only_link_is_accepted_at_definition():
    """A subclass overriding only ``static_url`` defines cleanly."""
    assert StaticLink().static_url == "https://example.com/static"


def test_link_source_inherited_from_intermediate_is_accepted():
    """The link source may come from an intermediate base."""
    assert ConcreteThroughIntermediate().get_link(None, ti_key=None) == "https://example.com/intermediate"


def test_concrete_link_without_link_source_fails_at_definition():
    """A concrete link overriding neither member fails at class definition, not at finalize."""
    with pytest.raises(TypeError, match=r"must override get_link\(\) or static_url"):

        class BrokenLink(BaseOperatorLink):
            @property
            def name(self) -> str:
                return "broken"


def test_abstract_intermediate_may_stay_incomplete():
    """An intermediate base that stays abstract is exempt (its children decide)."""

    class IncompleteIntermediate(BaseOperatorLink):
        pass

    assert IncompleteIntermediate.__abstractmethods__
