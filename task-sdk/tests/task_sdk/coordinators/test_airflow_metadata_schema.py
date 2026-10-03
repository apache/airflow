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
"""The bundle metadata JSON Schema accepts what the packers write, and the manifests older packers wrote."""

from __future__ import annotations

import copy
import json
import re
import textwrap
from typing import Any

import jsonschema
import pytest
import yaml

from tests_common.test_utils.paths import AIRFLOW_ROOT_PATH

DOCS_PATH = AIRFLOW_ROOT_PATH / "task-sdk" / "docs"
SCHEMA = json.loads((DOCS_PATH / "airflow-metadata.schema.json").read_text())

DIGESTS = {"integrity": "a" * 64, "cache": "b" * 64}

# What a packer writes today: no Dag inventory.
MANIFEST: dict[str, Any] = {
    "airflow_bundle_metadata_version": "1.0",
    "sdk": {"language": "go", "version": "0.1.0", "supervisor_schema_version": "2026-10-30"},
    "source": "main.go",
    "digests": DIGESTS,
}

# What a packer wrote before the manifest dropped the inventory.
MANIFEST_WITH_DAGS: dict[str, Any] = {
    **MANIFEST,
    "dags": {
        "simple_dag": {"tasks": ["extract", "transform", "load"]},
        "another_dag": {"tasks": []},
    },
}


def _is_valid(manifest: dict[str, Any]) -> bool:
    return jsonschema.Draft202012Validator(SCHEMA).is_valid(manifest)


def _get_spec_example() -> dict[str, Any]:
    """Return the manifest the executable bundle spec shows in its first YAML example."""
    spec = (DOCS_PATH / "executable-bundle-spec.rst").read_text()
    block = re.search(r"\.\. code-block:: yaml\n\n((?:    .*\n|\n)+)", spec)
    assert block is not None
    return yaml.safe_load(textwrap.dedent(block.group(1)))


def test_the_schema_is_a_valid_draft_2020_12_schema():
    jsonschema.Draft202012Validator.check_schema(SCHEMA)


@pytest.mark.parametrize("digests", [True, False], ids=["with-digests", "without-digests"])
def test_accepts_a_manifest_without_a_dag_inventory(digests):
    manifest = copy.deepcopy(MANIFEST)
    if not digests:
        del manifest["digests"]

    assert _is_valid(manifest)


def test_accepts_a_manifest_an_older_packer_wrote_with_its_dag_inventory():
    assert _is_valid(MANIFEST_WITH_DAGS)


def test_accepts_the_example_in_the_spec():
    example = _get_spec_example()

    assert "dags" not in example
    assert _is_valid(example)


def test_does_not_describe_a_dag_inventory():
    assert "dags" not in SCHEMA["properties"]
    assert "dags" not in SCHEMA["required"]
    assert "$defs" not in SCHEMA


def test_keeps_the_bundle_spec_version():
    assert (
        SCHEMA["properties"]["airflow_bundle_metadata_version"]["pattern"] == r"^[0-9]+\.[0-9]+(\.[0-9]+)?$"
    )
    assert MANIFEST["airflow_bundle_metadata_version"] == "1.0"
    assert "1.0" in SCHEMA["$id"]


@pytest.mark.parametrize("field", ["airflow_bundle_metadata_version", "sdk", "source"])
def test_requires_what_a_reader_needs(field):
    manifest = copy.deepcopy(MANIFEST)
    del manifest[field]

    assert not _is_valid(manifest)


@pytest.mark.parametrize("field", ["language", "version", "supervisor_schema_version"])
def test_requires_the_sdk_fields(field):
    manifest = copy.deepcopy(MANIFEST)
    del manifest["sdk"][field]

    assert not _is_valid(manifest)


def test_rejects_a_supervisor_schema_version_that_is_not_a_date():
    manifest = copy.deepcopy(MANIFEST)
    manifest["sdk"]["supervisor_schema_version"] = "latest"

    assert not _is_valid(manifest)
