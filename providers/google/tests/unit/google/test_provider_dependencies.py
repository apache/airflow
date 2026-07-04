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

from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]


def test_vertex_evaluation_dependencies_are_opt_in():
    provider_root = Path(__file__).parents[3]
    with (provider_root / "pyproject.toml").open("rb") as pyproject_file:
        project = tomllib.load(pyproject_file)["project"]

    aiplatform_dependencies = [
        dependency
        for dependency in project["dependencies"]
        if dependency.startswith("google-cloud-aiplatform")
    ]

    assert aiplatform_dependencies == ["google-cloud-aiplatform>=1.164.0"]
    assert project["optional-dependencies"]["vertex-eval"] == ["google-cloud-aiplatform[evaluation]>=1.164.0"]
