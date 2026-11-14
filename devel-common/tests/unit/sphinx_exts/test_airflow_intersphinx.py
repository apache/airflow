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

import sys
from pathlib import Path

import pytest

SPHINX_EXTS_PATH = Path(__file__).parents[3] / "src" / "sphinx_exts"
if SPHINX_EXTS_PATH.as_posix() not in sys.path:
    # The extensions are loaded by Sphinx from this directory and import each other by bare name.
    sys.path.append(SPHINX_EXTS_PATH.as_posix())

from sphinx_exts import airflow_intersphinx  # noqa: E402

PACKAGE = "apache-airflow-providers-ftp"


@pytest.fixture
def generated_path(tmp_path, monkeypatch):
    monkeypatch.setattr(airflow_intersphinx, "GENERATED_PATH", tmp_path)
    return tmp_path


def _touch(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")
    return path


class TestInventoryFor:
    def test_prefers_the_html_inventory_built_in_this_run(self, generated_path):
        build_dir = generated_path / "_build" / "docs" / PACKAGE / "stable"
        html = _touch(build_dir / "objects.inv")
        _touch(generated_path / "_inventory_cache" / PACKAGE / "objects.inv")

        assert airflow_intersphinx._inventory_for(PACKAGE, versioned=True) == html

    def test_falls_back_to_the_downloaded_inventory(self, generated_path):
        cached = generated_path / "_inventory_cache" / PACKAGE / "objects.inv"

        assert airflow_intersphinx._inventory_for(PACKAGE, versioned=True) == cached

    def test_non_versioned_packages_have_no_stable_directory(self, generated_path):
        html = _touch(generated_path / "_build" / "docs" / "docker-stack" / "objects.inv")

        assert airflow_intersphinx._inventory_for("docker-stack", versioned=False) == html
