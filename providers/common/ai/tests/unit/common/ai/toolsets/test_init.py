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

from airflow.providers.common.ai import toolsets


@pytest.mark.parametrize(
    ("name", "module"),
    [
        ("DataFusionToolset", "datafusion"),
        ("AgentSkillsToolset", "skills"),
        ("LoggingToolset", "logging"),
    ],
)
def test_toolset_is_exported_from_the_package(name, module):
    assert name in toolsets.__all__
    exported = getattr(toolsets, name)
    assert exported.__module__ == f"airflow.providers.common.ai.toolsets.{module}"
    assert exported.__name__ == name


def test_every_name_in_all_resolves():
    for name in toolsets.__all__:
        assert getattr(toolsets, name) is not None
