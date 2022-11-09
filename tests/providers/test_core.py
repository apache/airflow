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

import ast
from collections import defaultdict
from pathlib import Path

import pytest
import semver
import yaml

import airflow
from airflow.providers_manager import ProvidersManager
from tests.test_utils.providers import get_provider_min_airflow_version

repo_root = Path(airflow.__file__).parent.parent


def get_version_kwarg(name, kwargs, ref):
    raw = kwargs.get(name)
    if not raw:
        return None
    if not isinstance(raw, tuple):
        raise ValueError(
            f"File {ref} has call to warn_provider_deprecation"
            f"with improper arguments. `{name}` must be tuple."
        )
    provider_version = semver.VersionInfo(*raw)
    return provider_version


def iter_providers():
    for name, info in ProvidersManager().providers.items():
        package_name = info.data["package-name"]
        rel_path = Path(*package_name.replace("apache-airflow-providers-", "").split("-"))
        curr_prov_version = semver.VersionInfo.parse(info.version)
        curr_min_airflow_version = get_provider_min_airflow_version(package_name, info.data)
        provider_root = repo_root / "airflow/providers" / rel_path
        yield curr_prov_version, curr_min_airflow_version, provider_root, info


def test_provider_deprecation_removal():
    failures = {}
    for curr_prov_version, curr_min_airflow_version, provider_root, info in iter_providers():
        provider_failures = defaultdict(list)
        package_name = info.data["package-name"]
        for file in provider_root.rglob("*.py"):
            content = file.read_text()
            file_rel_path = file.relative_to(repo_root)
            tree = ast.parse(content)
            for elem in [
                x
                for x in ast.walk(tree)
                if isinstance(x, ast.Call)
                and getattr(x.func, "id", "").lstrip("_") == "warn_provider_deprecation"
            ]:
                kwargs = {y.arg: ast.literal_eval(y.value) for y in elem.keywords}
                ref = f"{file_rel_path}:{elem.lineno}"
                provider_version = get_version_kwarg("provider_version", kwargs, ref)
                if provider_version and curr_prov_version >= provider_version:
                    provider_failures["provider version exceeded"].append(ref)
                min_airflow_version = get_version_kwarg("min_airflow_version", kwargs, ref)
                if min_airflow_version and curr_min_airflow_version >= min_airflow_version:
                    provider_failures["min airflow version exceeded"].append(ref)
                if not (min_airflow_version or provider_version):
                    raise RuntimeError(
                        f"`warn_provider_deprecation` is called at {ref} but "
                        "neither provider_version nor min_airflow_version is specified."
                    )
            if provider_failures:
                failures[package_name] = dict(provider_failures)
    if failures:
        message = (
            "Found providers raising deprecation warning which must be addressed now.\n"
            "There are two classes of warnings. One is when the provider version is now\n"
            "greater than that specified in the warning.\n"
            "The other is when the current min airflow version for the provider is greater\n"
            "than specified in the warning.\n\n"
        )
        message += yaml.dump(dict(failures))
        pytest.fail(message)


def test_code_removal_todo_comments():
    failures = {}
    for curr_prov_version, curr_min_airflow_version, provider_root, info in iter_providers():
        provider_failures = defaultdict(list)
        package_name = info.data["package-name"]
        for file in provider_root.rglob("*.py"):
            for idx, line in enumerate(file.read_text().splitlines()):
                if line.startswith("# todo: remove-on-min-airflow-version="):
                    rel_path = file.relative_to(repo_root)
                    ref = f"{rel_path}:{idx + 1}"
                    version_str = line.split("=", maxsplit=1)[1]
                    version = tuple(map(int, version_str.split(".")))
                    if version and curr_min_airflow_version >= version:
                        provider_failures["min airflow version exceeded"].append(ref)
                if line.startswith("# todo: remove-on-provider-version="):
                    rel_path = file.relative_to(repo_root)
                    ref = f"{rel_path}:{idx + 1}"
                    version_str = line.split("=", maxsplit=1)[1]
                    version = tuple(map(int, version_str.split(".")))
                    if version and curr_prov_version >= version:
                        provider_failures["provider version exceeded"].append(ref)
        if provider_failures:
            failures[package_name] = dict(provider_failures)
    if failures:
        message = (
            "Found providers raising deprecation warning which must be addressed now.\n"
            "There are two classes of warnings. One is when the provider version is now\n"
            "greater than that specified in the warning.\n"
            "The other is when the current min airflow version for the provider is greater\n"
            "than specified in the warning.\n\n"
        )
        message += yaml.dump(dict(failures))
        pytest.fail(message)
