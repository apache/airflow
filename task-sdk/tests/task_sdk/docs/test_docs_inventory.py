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

# Add the SDK src directory to sys.path so that importlib loads our airflow.sdk module
sys.path.insert(0, str(Path(__file__).parents[3] / "src"))
import importlib
import shutil
import subprocess
import zlib
from pathlib import Path

import pytest


def read_inventory(inv_path: Path):
    """
    Read a Sphinx objects.inv inventory file and return a mapping of documented full names to their entries.
    """
    inv: dict[str, tuple[str, str, str]] = {}
    with inv_path.open("rb") as f:
        f.readline()
        f.readline()
        f.readline()
        f.readline()
        data = zlib.decompress(f.read()).decode("utf-8").splitlines()
    for line in data:
        if not line.strip():
            continue
        parts = line.split(None, 4)
        if len(parts) != 5:
            continue
        name, domain_role, prio, location, dispname = parts
        inv[name] = (domain_role, location, dispname)
    return inv


@pytest.mark.skipif(
    shutil.which("sphinx-build") is None, reason="sphinx-build not available, skipping docs inventory test"
)
def test_docs_inventory_matches_public_api(tmp_path):
    """
    Build the HTML docs and compare the generated Sphinx inventory with the public API re-exports.
    """
    docs_dir = Path(__file__).parents[3] / "docs"
    build_dir = tmp_path / "build"
    sphinx = shutil.which("sphinx-build")
    subprocess.run([sphinx, "-b", "html", "-q", str(docs_dir), str(build_dir)], check=True)
    inv_path = build_dir / "objects.inv"
    assert inv_path.exists(), "objects.inv not found after docs build"

    inv = read_inventory(inv_path)
    documented = {
        name.rsplit(".", 1)[-1]
        for name in inv.keys()
        if name.startswith("airflow.sdk.") and name.count(".") == 2
    }
    sdk = importlib.import_module("airflow.sdk")
    public = set(getattr(sdk, "__all__", [])) - {"__version__"}

    extras = {"AirflowParsingContext"}
    # we do not want to document the class for `conf` but a description is present in the docs
    excluded_from_docs = {"conf"}
    missing = (public - documented) - excluded_from_docs
    assert not missing, f"Public API items missing in docs: {missing}"
    unexpected = (documented - public) - extras
    assert not unexpected, f"Unexpected documented items: {unexpected}"
