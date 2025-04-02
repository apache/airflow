
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

<<<<<<<< HEAD:devel-common/src/tests_common/test_utils/markers.py
from __future__ import annotations

import os

import pytest

skip_if_force_lowest_dependencies_marker = pytest.mark.skipif(
    os.environ.get("FORCE_LOWEST_DEPENDENCIES", "") == "true",
    reason="When lowest dependencies are set only some providers are loaded",
)
========
[build-system]
requires = [ "hatchling==1.27.0" ]
build-backend = "hatchling.build"

[project]
name = "apache-airflow-helm-tests"
description = "Helm tests for Apache Airflow"
classifiers = [
    "Private :: Do Not Upload",
]
requires-python = "~=3.9,<3.13"
authors = [
    { name = "Apache Software Foundation", email = "dev@airflow.apache.org" },
]
maintainers = [
    { name = "Apache Software Foundation", email="dev@airflow.apache.org" },
]
version = "0.0.1"
dependencies = [
    "apache-airflow-devel-common",
    "apache-airflow-providers-cncf-kubernetes",
]

[tool.pytest.ini_options]
addopts = "-rasl --verbosity=2 -p no:flaky -p no:nose -p no:legacypath"
norecursedirs = [
    ".eggs",
]
log_level = "INFO"
filterwarnings = [
    "error::pytest.PytestCollectionWarning",
]
python_files = [
    "*.py",
]

# Keep temporary directories (created by `tmp_path`) for 2 recent runs only failed tests.
tmp_path_retention_count = "2"
tmp_path_retention_policy = "failed"

[tool.hatch.build.targets.sdist]
exclude = ["*"]

[tool.hatch.build.targets.wheel]
bypass-selection = true
>>>>>>>> 28b9ce5f6a (Simplify tooling by switching completely to uv (#48223)):helm-tests/pyproject.toml
