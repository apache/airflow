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
"""Unit tests for dev/registry/registry_contract_models.py."""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from registry_contract_models import (
    build_openapi_document,
    build_schema_documents,
    validate_provider_parameters,
    validate_provider_version_metadata,
    validate_provider_versions,
)


def test_validate_provider_versions_requires_latest_in_versions():
    with pytest.raises(ValidationError):
        validate_provider_versions({"latest": "2.0.0", "versions": ["1.0.0"]})


def test_validate_provider_parameters_preserves_mro_alias():
    payload = {
        "provider_id": "test",
        "provider_name": "Test",
        "version": "1.0.0",
        "generated_at": "2026-02-18T00:00:00+00:00",
        "classes": {
            "airflow.providers.test.mod.Example": {
                "name": "Example",
                "type": "operator",
                "mro": ["airflow.providers.test.mod.Example"],
                "parameters": [
                    {
                        "name": "value",
                        "type": "str",
                        "default": None,
                        "required": True,
                        "origin": "airflow.providers.test.mod.Example",
                        "description": None,
                    }
                ],
            }
        },
    }
    validated = validate_provider_parameters(payload)
    class_entry = validated["classes"]["airflow.providers.test.mod.Example"]
    assert "mro" in class_entry
    assert "mro_chain" not in class_entry


def test_validate_version_metadata_accepts_legacy_version_modules_without_ids():
    payload = {
        "provider_id": "test",
        "version": "0.9.0",
        "generated_at": "2026-02-18T00:00:00+00:00",
        "requires_python": ">=3.10",
        "dependencies": [],
        "optional_extras": {},
        "connection_types": [{"conn_type": "test", "hook_class": "x.y.Hook"}],
        "module_counts": {"operator": 1},
        "modules": [
            {
                "name": "LegacyOperator",
                "type": "operator",
                "import_path": "airflow.providers.test.operators.legacy.LegacyOperator",
                "short_description": "Legacy module shape from older backfills.",
                "docs_url": "https://example.invalid/docs",
                "source_url": "https://example.invalid/source",
                "category": "test",
            }
        ],
    }
    validated = validate_provider_version_metadata(payload)
    assert validated["modules"][0]["id"] is None
    assert validated["modules"][0]["provider_id"] is None
    assert validated["modules"][0]["provider_name"] is None


def test_schema_documents_include_expected_artifact_names():
    schemas = build_schema_documents()
    assert "providers-catalog.schema.json" in schemas
    assert "modules-catalog.schema.json" in schemas
    assert "provider-version-metadata.schema.json" in schemas
    assert "provider-connections.schema.json" in schemas
    assert "provider-versions.schema.json" in schemas


def test_openapi_document_uses_contract_component_references():
    openapi = build_openapi_document()
    schema_ref = openapi["paths"]["/api/providers/{providerId}/versions.json"]["get"]["responses"]["200"][
        "content"
    ]["application/json"]["schema"]["$ref"]
    assert schema_ref == "#/components/schemas/ProviderVersionsPayload"
