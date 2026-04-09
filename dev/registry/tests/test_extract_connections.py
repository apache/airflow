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
"""Unit tests for dev/registry/extract_connections.py."""

from __future__ import annotations

from extract_connections import (
    DEFAULT_LABELS,
    STANDARD_FIELDS,
    build_custom_fields,
    build_standard_fields,
    package_name_to_provider_id,
)


class TestPackageNameToProviderId:
    def test_standard_provider(self):
        assert package_name_to_provider_id("apache-airflow-providers-amazon") == "amazon"

    def test_nested_provider(self):
        assert package_name_to_provider_id("apache-airflow-providers-apache-beam") == "apache-beam"

    def test_unknown_prefix(self):
        assert package_name_to_provider_id("some-other-package") == "some-other-package"


class TestBuildStandardFields:
    def test_no_behaviour_returns_all_visible(self):
        result = build_standard_fields(None)
        assert len(result) == len(STANDARD_FIELDS)
        for field_name in STANDARD_FIELDS:
            assert result[field_name]["visible"] is True
            assert result[field_name]["label"] == DEFAULT_LABELS[field_name]

    def test_hidden_fields(self):
        behaviour = {"hidden_fields": ["host", "schema", "port"]}
        result = build_standard_fields(behaviour)
        assert result["host"]["visible"] is False
        assert result["schema"]["visible"] is False
        assert result["port"]["visible"] is False
        assert result["login"]["visible"] is True

    def test_relabeling(self):
        behaviour = {"relabeling": {"host": "Hostname", "login": "Username"}}
        result = build_standard_fields(behaviour)
        assert result["host"]["label"] == "Hostname"
        assert result["login"]["label"] == "Username"
        assert result["password"]["label"] == "Password"

    def test_placeholders(self):
        behaviour = {"placeholders": {"host": "my-host.example.com", "port": "5432"}}
        result = build_standard_fields(behaviour)
        assert result["host"]["placeholder"] == "my-host.example.com"
        assert result["port"]["placeholder"] == "5432"
        assert "placeholder" not in result["login"]

    def test_combined_behaviour(self):
        behaviour = {
            "hidden_fields": ["extra"],
            "relabeling": {"host": "Server"},
            "placeholders": {"host": "db.example.com"},
        }
        result = build_standard_fields(behaviour)
        assert result["extra"]["visible"] is False
        assert result["host"]["label"] == "Server"
        assert result["host"]["placeholder"] == "db.example.com"
        assert result["host"]["visible"] is True


class TestBuildCustomFields:
    def test_empty_widgets(self):
        result = build_custom_fields({}, "postgres")
        assert result == {}

    def test_filters_by_connection_type(self):
        """Only widgets matching the connection type prefix are included."""

        class FakeWidgetInfo:
            field_name = "region"
            field = {"schema": {"type": "string", "title": "Region"}, "value": "us-east-1"}
            is_sensitive = False

        class OtherWidgetInfo:
            field_name = "bucket"
            field = {"schema": {"type": "string", "title": "Bucket"}}
            is_sensitive = False

        widgets = {
            "extra__aws__region": FakeWidgetInfo(),
            "extra__gcp__bucket": OtherWidgetInfo(),
        }
        result = build_custom_fields(widgets, "aws")
        assert "region" in result
        assert "bucket" not in result

    def test_yaml_field_format(self):
        """YAML-sourced fields (dicts) are handled correctly."""

        class YamlWidgetInfo:
            field_name = "database"
            field = {
                "schema": {"type": "string", "title": "Database Name", "format": "password"},
                "value": "default_db",
                "description": "The database to connect to",
            }
            is_sensitive = True

        widgets = {"extra__ydb__database": YamlWidgetInfo()}
        result = build_custom_fields(widgets, "ydb")

        assert result["database"]["label"] == "Database Name"
        assert result["database"]["type"] == "string"
        assert result["database"]["default"] == "default_db"
        assert result["database"]["format"] == "password"
        assert result["database"]["description"] == "The database to connect to"
        assert result["database"]["is_sensitive"] is True

    def test_schema_enum(self):
        """Enum values in schema are preserved."""

        class EnumWidgetInfo:
            field_name = "auth_type"
            field = {
                "schema": {"type": "string", "title": "Auth Type", "enum": ["basic", "oauth"]},
            }
            is_sensitive = False

        widgets = {"extra__test__auth_type": EnumWidgetInfo()}
        result = build_custom_fields(widgets, "test")
        assert result["auth_type"]["enum"] == ["basic", "oauth"]
