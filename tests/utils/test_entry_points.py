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
#
from typing import Iterable
from unittest import mock

from airflow.utils.entry_points import entry_points_with_dist, metadata


class MockDistribution:
    def __init__(self, name: str, entry_points: Iterable[metadata.EntryPoint]) -> None:
        self.metadata = {"Name": name}
        self.entry_points = entry_points


class MockMetadata:
    def distributions(self):
        return [
            MockDistribution(
                "dist1",
                [metadata.EntryPoint("a", "b", "group_x"), metadata.EntryPoint("c", "d", "group_y")],
            ),
            MockDistribution("Dist2", [metadata.EntryPoint("e", "f", "group_x")]),
            MockDistribution("dist2", [metadata.EntryPoint("g", "h", "group_x")]),  # Duplicated name.
        ]


@mock.patch("airflow.utils.entry_points.metadata", MockMetadata())
def test_entry_points_with_dist():
    entries = list(entry_points_with_dist("group_x"))

    # The second "dist2" is ignored. Only "group_x" entries are loaded.
    assert [dist.metadata["Name"] for _, dist in entries] == ["dist1", "Dist2"]
    assert [ep.name for ep, _ in entries] == ["a", "e"]
