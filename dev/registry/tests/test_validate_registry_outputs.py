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
"""Unit tests for dev/registry/validate_registry_outputs.py."""

from __future__ import annotations

import json
from pathlib import Path

from registry_contract_models import (
    build_schema_documents,
    validate_modules_catalog,
    validate_provider_connections,
    validate_provider_parameters,
    validate_provider_version_metadata,
    validate_provider_versions,
    validate_providers_catalog,
)
from validate_registry_outputs import validate_outputs


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def _write_schema_artifacts(schema_dir: Path) -> None:
    for filename, schema in build_schema_documents().items():
        _write_json(schema_dir / filename, schema)


def _sample_provider(provider_id: str = "test", version: str = "1.0.0") -> dict:
    return {
        "id": provider_id,
        "name": "Test Provider",
        "package_name": "apache-airflow-providers-test",
        "description": "Test provider",
        "lifecycle": "production",
        "logo": None,
        "version": version,
        "versions": [version],
        "airflow_versions": ["3.0+"],
        "pypi_downloads": {"weekly": 1, "monthly": 2, "total": 0},
        "module_counts": {"operator": 1},
        "categories": [{"id": "test", "name": "Test", "module_count": 1}],
        "connection_types": [],
        "requires_python": ">=3.10",
        "dependencies": [],
        "optional_extras": {},
        "dependents": [],
        "related_providers": [],
        "docs_url": "https://example.invalid/docs",
        "source_url": "https://example.invalid/source",
        "pypi_url": "https://example.invalid/pypi",
        "first_released": "2026-01-01",
        "last_updated": "2026-01-01",
    }


def test_validate_outputs_succeeds_for_valid_payloads(tmp_path):
    schema_dir = tmp_path / "schemas"
    data_dir = tmp_path / "data"
    site_api_dir = tmp_path / "_site" / "api"
    _write_schema_artifacts(schema_dir)

    provider = _sample_provider()
    modules_payload = validate_modules_catalog(
        {
            "modules": [
                {
                    "id": "test-test-ExampleOperator",
                    "name": "ExampleOperator",
                    "type": "operator",
                    "import_path": "airflow.providers.test.operators.example.ExampleOperator",
                    "module_path": "airflow.providers.test.operators.example",
                    "short_description": "Example operator",
                    "docs_url": "https://example.invalid/docs",
                    "source_url": "https://example.invalid/source",
                    "category": "test",
                    "provider_id": "test",
                    "provider_name": "Test Provider",
                }
            ]
        }
    )

    parameters_payload = validate_provider_parameters(
        {
            "provider_id": "test",
            "provider_name": "Test Provider",
            "version": "1.0.0",
            "generated_at": "2026-02-18T00:00:00+00:00",
            "classes": {
                "airflow.providers.test.operators.example.ExampleOperator": {
                    "name": "ExampleOperator",
                    "type": "operator",
                    "mro": ["airflow.providers.test.operators.example.ExampleOperator"],
                    "parameters": [],
                }
            },
        }
    )

    connections_payload = validate_provider_connections(
        {
            "provider_id": "test",
            "provider_name": "Test Provider",
            "version": "1.0.0",
            "generated_at": "2026-02-18T00:00:00+00:00",
            "connection_types": [
                {
                    "connection_type": "test",
                    "hook_class": "airflow.providers.test.hooks.example.ExampleHook",
                    "standard_fields": {
                        "host": {"visible": True, "label": "Host"},
                    },
                    "custom_fields": {},
                }
            ],
        }
    )

    metadata_payload = validate_provider_version_metadata(
        {
            "provider_id": "test",
            "version": "1.0.0",
            "generated_at": "2026-02-18T00:00:00+00:00",
            "requires_python": ">=3.10",
            "dependencies": [],
            "optional_extras": {},
            "connection_types": [{"conn_type": "test", "hook_class": "x.y.Hook"}],
            "module_counts": {"operator": 1},
            "modules": modules_payload["modules"],
        }
    )

    _write_json(data_dir / "providers.json", validate_providers_catalog({"providers": [provider]}))
    _write_json(data_dir / "modules.json", modules_payload)
    version_dir = data_dir / "versions" / "test" / "1.0.0"
    _write_json(version_dir / "metadata.json", metadata_payload)
    _write_json(version_dir / "parameters.json", parameters_payload)
    _write_json(version_dir / "connections.json", connections_payload)
    _write_json(
        site_api_dir / "providers" / "test" / "versions.json",
        validate_provider_versions({"latest": "1.0.0", "versions": ["1.0.0"]}),
    )

    assert (
        validate_outputs(
            data_dir=data_dir,
            schema_dir=schema_dir,
            strict=True,
            site_api_dir=site_api_dir,
        )
        == 0
    )


def test_validate_outputs_fails_when_required_file_has_invalid_shape(tmp_path):
    schema_dir = tmp_path / "schemas"
    data_dir = tmp_path / "data"
    _write_schema_artifacts(schema_dir)

    _write_json(data_dir / "providers.json", {})
    _write_json(data_dir / "modules.json", validate_modules_catalog({"modules": []}))

    assert validate_outputs(data_dir=data_dir, schema_dir=schema_dir) == 1


def test_validate_outputs_strict_fails_when_versioned_payloads_are_missing(tmp_path):
    schema_dir = tmp_path / "schemas"
    data_dir = tmp_path / "data"
    _write_schema_artifacts(schema_dir)

    provider = _sample_provider()
    _write_json(data_dir / "providers.json", validate_providers_catalog({"providers": [provider]}))
    _write_json(data_dir / "modules.json", validate_modules_catalog({"modules": []}))

    assert validate_outputs(data_dir=data_dir, schema_dir=schema_dir, strict=True) == 1


def test_validate_outputs_fails_for_invalid_site_versions_payload(tmp_path):
    schema_dir = tmp_path / "schemas"
    data_dir = tmp_path / "data"
    site_api_dir = tmp_path / "_site" / "api"
    _write_schema_artifacts(schema_dir)

    provider = _sample_provider()
    _write_json(data_dir / "providers.json", validate_providers_catalog({"providers": [provider]}))
    _write_json(data_dir / "modules.json", validate_modules_catalog({"modules": []}))
    _write_json(site_api_dir / "providers" / "test" / "versions.json", {})

    assert validate_outputs(data_dir=data_dir, schema_dir=schema_dir, site_api_dir=site_api_dir) == 1
