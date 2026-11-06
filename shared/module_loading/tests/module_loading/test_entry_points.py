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

from collections.abc import Iterable
from unittest import mock

import pytest

from airflow_shared.module_loading import _get_grouped_entry_points, entry_points_with_dist, metadata


class MockDistribution:
    def __init__(
        self, name: str, entry_points: Iterable[metadata.EntryPoint], version: str = "1.0.0"
    ) -> None:
        self.metadata: dict[str, str] | None = {"Name": name}
        self.entry_points = entry_points
        self.version = version


class MockMetadata:
    def __init__(self, distributions: list[MockDistribution]) -> None:
        self._distributions = distributions

    def distributions(self):
        return self._distributions


@pytest.fixture(autouse=True)
def clear_entry_points_cache():
    _get_grouped_entry_points.cache_clear()
    yield
    _get_grouped_entry_points.cache_clear()


@mock.patch(
    "airflow_shared.module_loading.metadata",
    MockMetadata(
        [
            MockDistribution(
                "dist1",
                [metadata.EntryPoint("a", "b", "group_x"), metadata.EntryPoint("c", "d", "group_y")],
            ),
            MockDistribution("Dist2", [metadata.EntryPoint("e", "f", "group_x")]),
            MockDistribution("dist2", [metadata.EntryPoint("g", "h", "group_x")]),  # Duplicated name.
        ]
    ),
)
def test_entry_points_with_dist():
    entries = list(entry_points_with_dist("group_x"))

    # Only "group_x" entries are loaded, and only from the first "dist2" found.
    assert [dist.metadata["Name"] for _, dist in entries] == ["dist1", "Dist2"]
    assert [ep.name for ep, _ in entries] == ["a", "e"]


@mock.patch(
    "airflow_shared.module_loading.metadata",
    MockMetadata(
        [
            # The same distribution on two sys.path entries (e.g. a venv and the system
            # site-packages it falls back to), installed there in different versions.
            MockDistribution(
                "apache-airflow-providers-foo",
                [metadata.EntryPoint("foo", "airflow.providers.foo.plugins:FooPlugin", "airflow.plugins")],
                version="2.0.0",
            ),
            MockDistribution(
                "apache_airflow_providers_foo",
                [metadata.EntryPoint("foo", "airflow.providers.foo.plugin:FooPlugin", "airflow.plugins")],
                version="1.0.0",
            ),
        ]
    ),
)
def test_entry_points_with_dist_same_dist_in_two_versions_returns_first_only():
    entries = list(entry_points_with_dist("airflow.plugins"))

    assert [(ep.value, dist.version) for ep, dist in entries] == [
        ("airflow.providers.foo.plugins:FooPlugin", "2.0.0")
    ]


# No metadata file at all, or metadata without a Name field (the key is absent, not None).
@pytest.mark.parametrize("dist_metadata", [None, {"Version": "1.0.0"}], ids=["no-metadata", "no-name"])
def test_entry_points_with_dist_without_name_are_not_deduplicated(dist_metadata):
    distributions = [
        MockDistribution("unnamed", [metadata.EntryPoint("a", "b", "group_x")]),
        MockDistribution("unnamed", [metadata.EntryPoint("c", "d", "group_x")]),
    ]
    for dist in distributions:
        dist.metadata = dist_metadata

    with mock.patch("airflow_shared.module_loading.metadata", MockMetadata(distributions)):
        entries = list(entry_points_with_dist("group_x"))

    assert [ep.name for ep, _ in entries] == ["a", "c"]
