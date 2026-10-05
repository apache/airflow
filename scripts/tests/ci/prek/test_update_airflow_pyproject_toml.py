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
from ci.prek.update_airflow_pyproject_toml import get_exclusion_marker
from packaging.requirements import Requirement


@pytest.mark.parametrize("version", ["3.10.0", "3.11.0"])
def test_get_exclusion_marker_for_patch(version):
    marker = get_exclusion_marker({"excluded-python-versions": [version]})

    assert marker == f'; python_full_version !=\\"{version}.*\\"'
    unescaped_marker = marker.replace("\\", "")
    requirement = Requirement(f"apache-airflow-providers-example>=1.0{unescaped_marker}")
    assert not requirement.marker.evaluate({"python_full_version": version})
    assert not requirement.marker.evaluate({"python_full_version": f"{version}.post1"})
    minor, patch = version.rsplit(".", 1)
    assert requirement.marker.evaluate({"python_full_version": f"{minor}.{int(patch) + 1}"})


def test_get_exclusion_marker_combines_patch_minor_and_platform():
    marker = get_exclusion_marker(
        {
            "excluded-python-versions": ["3.10.0", "3.14"],
            "excluded-platforms": ["linux/arm64"],
        }
    )

    assert 'python_full_version !=\\"3.10.0.*\\"' in marker
    assert 'python_version !=\\"3.14\\"' in marker
    assert 'platform_machine !=\\"aarch64\\"' in marker
    unescaped_marker = marker.replace("\\", "")
    requirement = Requirement(f"apache-airflow-providers-example>=1.0{unescaped_marker}")
    assert requirement.marker.evaluate(
        {"python_version": "3.10", "python_full_version": "3.10.1", "platform_machine": "x86_64"}
    )
    assert not requirement.marker.evaluate(
        {"python_version": "3.14", "python_full_version": "3.14.1", "platform_machine": "x86_64"}
    )
    assert not requirement.marker.evaluate(
        {"python_version": "3.10", "python_full_version": "3.10.1", "platform_machine": "aarch64"}
    )
