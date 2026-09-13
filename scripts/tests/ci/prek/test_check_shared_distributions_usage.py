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

import pytest
from ci.prek import check_shared_distributions_usage as shared_usage


@pytest.mark.parametrize("provider_path", ["providers/elasticsearch", "providers/microsoft/azure"])
def test_force_include_resolves_shared_source_from_provider(tmp_path, monkeypatch, provider_path):
    shared_dir = tmp_path / "shared"
    monkeypatch.setattr(shared_usage, "SHARED_DIR", shared_dir)
    provider_dir = tmp_path / provider_path
    provider_dir.mkdir(parents=True)
    pyproject = provider_dir / "pyproject.toml"
    pyproject.write_text('[project]\nname = "test-provider"\n')
    shared_folder = provider_dir / "src" / "airflow" / "providers" / "example" / "_shared"

    assert shared_usage.check_force_include(pyproject, ["apache-airflow-shared-search"], shared_folder) == []

    with pyproject.open("rb") as file:
        config = shared_usage.tomllib.load(file)
    includes = config["tool"]["hatch"]["build"]["targets"]["sdist"]["force-include"]
    source, destination = next(iter(includes.items()))
    assert (provider_dir / source).resolve() == shared_dir / "search/src/airflow_shared/search"
    assert provider_dir / destination == shared_folder / "search"
