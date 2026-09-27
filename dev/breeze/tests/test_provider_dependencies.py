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

import pytest

from airflow_breeze.utils import provider_dependencies as provider_dependencies_module
from airflow_breeze.utils.provider_dependencies import (
    generate_provider_dependencies_if_needed,
    get_related_providers,
)


def test_get_downstream_only():
    related_providers = get_related_providers(
        "trino", upstream_dependencies=False, downstream_dependencies=True
    )
    assert {"openlineage", "google", "common.sql", "common.compat"} == related_providers


def test_get_upstream_only():
    related_providers = get_related_providers(
        "trino", upstream_dependencies=True, downstream_dependencies=False
    )
    assert {"mysql", "google"} == related_providers


def test_both():
    related_providers = get_related_providers(
        "trino", upstream_dependencies=True, downstream_dependencies=True
    )
    assert {"openlineage", "google", "mysql", "common.sql", "common.compat"} == related_providers


def test_none():
    with pytest.raises(ValueError, match=r".*must be.*"):
        get_related_providers("trino", upstream_dependencies=False, downstream_dependencies=False)


@pytest.fixture
def provider_deps_files(tmp_path):
    """Point the module at a throwaway json/sha256sum pair and clear the lru_cache around it."""
    json_path = tmp_path / "provider_dependencies.json"
    hash_path = tmp_path / "provider_dependencies.json.sha256sum"
    provider_dependencies_module.get_provider_dependencies.cache_clear()
    with mock.patch.multiple(
        provider_dependencies_module,
        PROVIDER_DEPENDENCIES_JSON_PATH=json_path,
        PROVIDER_DEPENDENCIES_JSON_HASH_PATH=hash_path,
    ):
        yield json_path, hash_path
    provider_dependencies_module.get_provider_dependencies.cache_clear()


@pytest.mark.parametrize(
    "hash_sidecar_present",
    [
        pytest.param(False, id="missing-sidecar"),
        pytest.param(True, id="stale-sidecar"),
    ],
)
@mock.patch.object(provider_dependencies_module, "regenerate_provider_dependencies_once")
@mock.patch.object(provider_dependencies_module, "_calculate_provider_deps_hash")
def test_stale_dependencies_are_regenerated(
    mock_hash, mock_regenerate, provider_deps_files, hash_sidecar_present
):
    """A present-but-outdated json must be regenerated, and the sidecar refreshed.

    Without this, breeze silently enumerates providers from stale state - which dropped
    common.ai from the 2026-08-01 provider release wave.
    """
    json_path, hash_path = provider_deps_files
    json_path.write_text('{"common.ai": {"state": "not-ready"}}')
    if hash_sidecar_present:
        hash_path.write_text("stale-hash\n")
    mock_hash.return_value = "fresh-hash"

    def regenerate():
        json_path.write_text('{"common.ai": {"state": "ready"}}')

    mock_regenerate.side_effect = regenerate

    generate_provider_dependencies_if_needed()

    mock_regenerate.assert_called_once()
    assert provider_dependencies_module.get_provider_dependencies() == {"common.ai": {"state": "ready"}}
    assert hash_path.read_text().strip() == "fresh-hash"


@mock.patch.object(provider_dependencies_module, "regenerate_provider_dependencies_once")
@mock.patch.object(provider_dependencies_module, "_calculate_provider_deps_hash")
def test_up_to_date_dependencies_are_not_regenerated(mock_hash, mock_regenerate, provider_deps_files):
    json_path, hash_path = provider_deps_files
    json_path.write_text('{"common.ai": {"state": "ready"}}')
    hash_path.write_text("fresh-hash\n")
    mock_hash.return_value = "fresh-hash"

    generate_provider_dependencies_if_needed()

    mock_regenerate.assert_not_called()


@mock.patch.object(provider_dependencies_module, "regenerate_provider_dependencies_once")
@mock.patch.object(provider_dependencies_module, "_calculate_provider_deps_hash")
def test_hash_not_written_when_regeneration_fails(mock_hash, mock_regenerate, provider_deps_files):
    """A failed regeneration must not leave a sidecar vouching for stale content."""
    _, hash_path = provider_deps_files
    mock_hash.return_value = "fresh-hash"
    mock_regenerate.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        generate_provider_dependencies_if_needed()

    assert not hash_path.exists()
