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
"""Tests for cascading/dependent dropdown Param support."""
from __future__ import annotations

import copy

import pytest

from airflow.sdk.definitions.param import Param, ParamValidationError


class TestParamCascading:
    """Tests for depends_on / options_map cascading dropdown support."""

    def test_basic_creation(self):
        """Param with depends_on and options_map should store them in schema."""
        p = Param(
            "Nandi County",
            type="string",
            depends_on="country",
            options_map={
                "Kenya": ["Nandi County", "Nyandarua"],
                "Ethiopia": ["Jimma", "Arsi"],
            },
        )
        assert p.schema.get("depends_on") == "country"
        assert p.schema.get("options_map") == {
            "Kenya": ["Nandi County", "Nyandarua"],
            "Ethiopia": ["Jimma", "Arsi"],
        }

    def test_no_cascading_by_default(self):
        """Regular Param should not have cascading attributes in schema."""
        p = Param("hello", type="string")
        assert p.schema.get("depends_on") is None
        assert p.schema.get("options_map") is None

    def test_depends_on_without_options_map_raises(self):
        """depends_on without options_map should raise."""
        with pytest.raises(ParamValidationError, match="must also specify options_map"):
            Param("val", type="string", depends_on="parent")

    def test_options_map_without_depends_on_raises(self):
        """options_map without depends_on should raise."""
        with pytest.raises(ParamValidationError, match="must also specify depends_on"):
            Param("val", type="string", options_map={"A": ["val"]})

    def test_options_map_values_must_be_lists(self):
        """options_map values that are not lists should raise."""
        with pytest.raises(ParamValidationError, match="options_map values must be lists"):
            Param("val", type="string", depends_on="parent", options_map={"A": "not_a_list"})

    def test_serialize_includes_cascading(self):
        """Cascading attributes should be in schema during serialization."""
        p = Param("val", type="string", depends_on="parent", options_map={"A": ["val", "other"]})
        data = p.serialize()
        assert data["schema"]["depends_on"] == "parent"
        assert data["schema"]["options_map"] == {"A": ["val", "other"]}

    def test_serialize_excludes_cascading_when_absent(self):
        """Non-cascading params should not have cascading keys in schema."""
        p = Param("hello", type="string")
        data = p.serialize()
        assert "depends_on" not in data["schema"]
        assert "options_map" not in data["schema"]

    def test_deserialize_with_cascading(self):
        """Cascading attributes should survive a serialize-deserialize round trip."""
        original = Param("val", type="string", depends_on="parent", options_map={"A": ["val"]})
        data = original.serialize()
        restored = Param.deserialize(data, version=1)
        assert restored.schema.get("depends_on") == "parent"
        assert restored.schema.get("options_map") == {"A": ["val"]}

    def test_deserialize_without_cascading(self):
        """Non-cascading params should deserialize cleanly."""
        original = Param("hello", type="string")
        data = original.serialize()
        restored = Param.deserialize(data, version=1)
        assert restored.schema.get("depends_on") is None
        assert restored.schema.get("options_map") is None

    def test_dump_includes_cascading(self):
        """dump() should include cascading metadata in schema."""
        p = Param("val", type="string", depends_on="parent", options_map={"X": ["val"]})
        dumped = p.dump()
        assert dumped["schema"]["depends_on"] == "parent"
        assert dumped["schema"]["options_map"] == {"X": ["val"]}

    def test_dump_regular_param_no_cascading(self):
        """Regular params should not have cascading keys in schema dump."""
        p = Param("hello", type="string")
        dumped = p.dump()
        assert "depends_on" not in dumped["schema"]
        assert "options_map" not in dumped["schema"]

    def test_copy_preserves_cascading(self):
        """copy() should preserve cascading attributes in schema."""
        p = Param("val", type="string", depends_on="parent", options_map={"A": ["val"]})
        p_copy = copy.copy(p)
        assert p_copy.schema.get("depends_on") == "parent"
        assert p_copy.schema.get("options_map") == {"A": ["val"]}

    def test_cascading_attrs_in_schema(self):
        """depends_on and options_map should be stored in the schema dict."""
        p = Param("val", type="string", depends_on="parent", options_map={"A": ["val"]})
        assert p.schema.get("depends_on") == "parent"
        assert p.schema.get("options_map") == {"A": ["val"]}

    def test_resolve_still_works_with_cascading(self):
        """resolve() should work normally with cascading params."""
        p = Param("Nandi", type="string", depends_on="country", options_map={"Kenya": ["Nandi"]})
        assert p.resolve() == "Nandi"

    def test_chained_cascading(self):
        """Multiple levels of cascading should be independently valid."""
        country = Param("Kenya", type="string", enum=["Kenya", "Ethiopia"])
        region = Param(
            "Nandi",
            type="string",
            depends_on="country",
            options_map={"Kenya": ["Nandi", "Nakuru"], "Ethiopia": ["Jimma"]},
        )
        city = Param(
            "Kapsabet",
            type="string",
            depends_on="region",
            options_map={
                "Nandi": ["Kapsabet", "Nandi Hills"],
                "Nakuru": ["Nakuru Town"],
                "Jimma": ["Jimma City"],
            },
        )
        assert country.schema.get("depends_on") is None
        assert region.schema.get("depends_on") == "country"
        assert city.schema.get("depends_on") == "region"

    def test_empty_options_map_values(self):
        """Empty lists in options_map should be valid."""
        p = Param("val", type="string", depends_on="parent", options_map={"A": ["val"], "B": []})
        assert p.schema.get("options_map")["B"] == []
