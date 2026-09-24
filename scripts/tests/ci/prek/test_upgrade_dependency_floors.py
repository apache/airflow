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

import textwrap
from datetime import timedelta

import pytest
from common_prek_utils import AIRFLOW_ROOT_PATH
from upgrade_dependency_floors import FloorConfig, is_curated, load_config, parse_duration

CONFIG = """
[tool.airflow.dependency-floors]
min-age = "180 days"
packages = ["boto3", "botocore", "Google_Cloud-*"]
groups = [["boto3", "botocore"]]

[tool.airflow.dependency-floors.exclude]
Sagemaker_Studio = "Ask AWS first"
"""


def _write(tmp_path, content):
    path = tmp_path / "pyproject.toml"
    path.write_text(textwrap.dedent(content))
    return path


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("180 days", timedelta(days=180), id="days"),
        pytest.param("1 day", timedelta(days=1), id="singular"),
        pytest.param("12 hours", timedelta(hours=12), id="hours"),
        pytest.param("30 minutes", timedelta(minutes=30), id="minutes"),
        pytest.param(" 1.5 days ", timedelta(days=1.5), id="fraction-and-spaces"),
    ],
)
def test_parse_duration(value, expected):
    assert parse_duration(value) == expected


@pytest.mark.parametrize("value", ["", "6 months", "days", "-1 days"])
def test_parse_duration_rejects(value):
    with pytest.raises(ValueError, match="duration"):
        parse_duration(value)


def test_load_config_canonicalizes_patterns(tmp_path):
    config = load_config(_write(tmp_path, CONFIG))
    assert config == FloorConfig(
        min_age=timedelta(days=180),
        packages=("boto3", "botocore", "google-cloud-*"),
        groups=(("boto3", "botocore"),),
        exclude={"sagemaker-studio": "Ask AWS first"},
    )


def test_load_config_missing_section(tmp_path):
    with pytest.raises(ValueError, match=r"\[tool.airflow.dependency-floors\]"):
        load_config(_write(tmp_path, "[project]\nname = 'x'\n"))


def test_load_config_group_member_not_curated(tmp_path):
    content = CONFIG.replace('groups = [["boto3", "botocore"]]', 'groups = [["boto3", "aiobotocore"]]')
    with pytest.raises(ValueError, match="aiobotocore"):
        load_config(_write(tmp_path, content))


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("boto3", True, id="exact"),
        pytest.param("google-cloud-storage", True, id="glob"),
        pytest.param("google_cloud_storage", True, id="non-canonical"),
        pytest.param("google-api-core", False, id="no-match"),
    ],
)
def test_is_curated(tmp_path, name, expected):
    assert is_curated(name, load_config(_write(tmp_path, CONFIG))) is expected


def test_repository_config_loads():
    config = load_config(AIRFLOW_ROOT_PATH / "pyproject.toml")
    assert config.min_age == timedelta(days=180)
    assert ("boto3", "botocore") in config.groups
