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

import json

from ci.prek import update_helm_schema_schema_type_fields_match


def test_main_updates_both_leaf_type_matchers(tmp_path, monkeypatch):
    values_schema_file = tmp_path / "values.schema.json"
    values_schema_file.write_text(
        json.dumps(
            {
                "type": "object",
                "properties": {
                    "count": {"type": "integer"},
                    "nested": {
                        "type": "object",
                        "properties": {"optional": {"type": ["null", "string"]}},
                    },
                },
            }
        )
    )
    meta_schema_file = tmp_path / "values_schema.schema.json"
    meta_schema_file.write_text(
        json.dumps(
            {
                "$defs": {
                    "leaf": {
                        "if": {"oneOf": []},
                        "properties": {"properties": {"additionalProperties": {"if": {"oneOf": []}}}},
                    }
                }
            }
        )
    )
    monkeypatch.setattr(
        update_helm_schema_schema_type_fields_match, "_VALUES_SCHEMA_FILE", values_schema_file
    )
    monkeypatch.setattr(
        update_helm_schema_schema_type_fields_match, "_VALUES_SCHEMA_SCHEMA_FILE", meta_schema_file
    )

    assert update_helm_schema_schema_type_fields_match.main() == 0

    leaf_definition = json.loads(meta_schema_file.read_text())["$defs"]["leaf"]
    nested_matcher = leaf_definition["properties"]["properties"]["additionalProperties"]["if"]["oneOf"]
    outer_matcher = leaf_definition["if"]["oneOf"]
    matched_types = [match["properties"]["type"]["const"] for match in nested_matcher]

    assert outer_matcher == nested_matcher
    assert "integer" in matched_types
    assert "object" in matched_types
    assert ["null", "string"] in matched_types
    object_matcher = next(
        match for match in nested_matcher if match["properties"]["type"]["const"] == "object"
    )
    assert object_matcher["properties"]["properties"] is False
