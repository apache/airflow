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

from airflow_breeze.utils.import_graph import find_reachable_providers

ALPHA = "providers/alpha/src/airflow/providers/alpha"


@pytest.fixture
def repo(tmp_path):
    files = {
        "airflow-core/src/airflow/config_templates/config.yml": (
            "core:\n  options:\n    x:\n      default: airflow.providers.alpha.configured\n"
        ),
        "providers/alpha/pyproject.toml": (
            "[project]\nname = 'alpha'\n"
            "[project.entry-points.apache_airflow_provider]\nprovider_info = 'airflow.providers.alpha.entry:get'\n"
        ),
        "providers/alpha/provider.yaml": (
            "operators:\n  - python-modules: [airflow.providers.alpha.operators]\n"
            "connection-types:\n  - hook-class-name: airflow.providers.alpha.hooks.AlphaHook\n"
        ),
        f"{ALPHA}/__init__.py": "",
        f"{ALPHA}/operators.py": "from .helpers import x\n",
        f"{ALPHA}/helpers.py": "",
        f"{ALPHA}/hooks.py": "from airflow.providers.alpha import hook_utils\n",
        f"{ALPHA}/hook_utils.py": "",
        f"{ALPHA}/configured.py": "",
        f"{ALPHA}/entry.py": "",
        f"{ALPHA}/loaded.py": "",
        f"{ALPHA}/loader.py": "import_string('airflow.providers.alpha.loaded')\n",
        f"{ALPHA}/broken.py": "def (\n",
        f"{ALPHA}/runtime_only.py": "",
        f"{ALPHA}/relative_loaded.py": "",
        f"{ALPHA}/from_listed.py": "",
        f"{ALPHA}/star/__init__.py": "__all__ = ['starred']\n",
        f"{ALPHA}/star/starred.py": "",
        f"{ALPHA}/dynamic_star/__init__.py": "__all__ = ['dynamic_starred']\n",
        f"{ALPHA}/dynamic_star/dynamic_starred.py": "",
        "providers/beta/tests/unit/beta/fixtures/bad_project/pyproject.toml": "not [ toml\n",
        f"{ALPHA}/get_provider_info.py": "x = 'airflow.providers.alpha.unused'\n",
        f"{ALPHA}/migrations/versions/m1.py": "",
        f"{ALPHA}/typed.py": "",
        f"{ALPHA}/patched.py": "",
        f"{ALPHA}/unused.py": "",
        "providers/alpha/tests/conftest.py": "",
        "providers/beta/pyproject.toml": "[project]\nname = 'beta'\n",
        "providers/beta/provider.yaml": "package-name: beta\n",
        "providers/beta/tests/unit/beta/test_beta.py": (
            "from typing import TYPE_CHECKING\n"
            "from airflow.providers.alpha import operators\n"
            "if TYPE_CHECKING:\n"
            "    from airflow.providers.alpha import typed\n"
            "if not TYPE_CHECKING:\n"
            "    from airflow.providers.alpha import runtime_only\n"
            "from airflow.providers.alpha.star import *\n"
        ),
        "providers/gamma/pyproject.toml": "[project]\nname = 'gamma'\n",
        "providers/gamma/provider.yaml": "package-name: gamma\n",
        "providers/gamma/tests/unit/gamma/test_gamma.py": (
            "from unittest import mock\n"
            "@mock.patch('airflow.providers.alpha.patched.thing')\n"
            "def test_it(m): ...\n"
            "importlib.import_module('.relative_loaded', package='airflow.providers.alpha')\n"
            "__import__('airflow.providers.alpha', fromlist=['from_listed'])\n"
            "__import__('airflow.providers.alpha.dynamic_star', fromlist=['*'])\n"
        ),
    }
    for path, content in files.items():
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_text(content)
    return tmp_path


@pytest.mark.parametrize(
    ("changed", "expected"),
    [
        pytest.param(f"{ALPHA}/operators.py", {"beta"}, id="imported-module-keeps-importer"),
        pytest.param(f"{ALPHA}/helpers.py", {"beta"}, id="relative-import-is-an-edge"),
        pytest.param(f"{ALPHA}/runtime_only.py", {"beta"}, id="not-type-checking-import-is-an-edge"),
        pytest.param(f"{ALPHA}/star/starred.py", {"beta"}, id="wildcard-import-reaches-child-modules"),
        pytest.param(
            f"{ALPHA}/dynamic_star/dynamic_starred.py", None, id="dunder-import-wildcard-falls-back"
        ),
        pytest.param(f"{ALPHA}/unused.py", set(), id="unimported-module-drops-all"),
        pytest.param(f"{ALPHA}/typed.py", set(), id="type-checking-import-is-no-edge"),
        pytest.param(f"{ALPHA}/patched.py", {"gamma"}, id="mock-patch-string-is-an-edge"),
        pytest.param(f"{ALPHA}/hooks.py", None, id="runtime-provider-yaml-key-falls-back"),
        pytest.param(f"{ALPHA}/hook_utils.py", None, id="import-of-dynamic-module-falls-back"),
        pytest.param(f"{ALPHA}/__init__.py", None, id="package-of-dynamic-module-falls-back"),
        pytest.param(f"{ALPHA}/relative_loaded.py", None, id="relative-import-module-falls-back"),
        pytest.param(f"{ALPHA}/from_listed.py", None, id="dunder-import-fromlist-falls-back"),
        pytest.param(f"{ALPHA}/configured.py", None, id="config-yml-falls-back"),
        pytest.param(f"{ALPHA}/entry.py", None, id="entry-point-falls-back"),
        pytest.param(f"{ALPHA}/loaded.py", None, id="import-string-literal-falls-back"),
        pytest.param(f"{ALPHA}/broken.py", None, id="unparsable-file-falls-back"),
        pytest.param(f"{ALPHA}/migrations/versions/m1.py", None, id="file-loaded-by-path-falls-back"),
        pytest.param("providers/alpha/tests/conftest.py", None, id="conftest-falls-back"),
        pytest.param("providers/alpha/provider.yaml", None, id="provider-yaml-falls-back"),
        pytest.param("devel-common/src/tests_common/x.py", None, id="devel-common-falls-back"),
        pytest.param("providers/alpha/docs/index.rst", set(), id="docs-are-ignored"),
    ],
)
def test_find_reachable_providers(repo, changed, expected):
    assert find_reachable_providers(repo, (changed,), {"beta", "gamma"}) == expected
