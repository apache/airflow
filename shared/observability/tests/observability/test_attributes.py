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

from airflow_shared.observability.attributes import expand_dag_tags


@pytest.mark.parametrize(
    ("tag_names", "expected"),
    [
        pytest.param([], {}, id="empty"),
        pytest.param(["production"], {"production": ""}, id="standalone"),
        pytest.param(["env:prod"], {"env": "prod"}, id="key-value"),
        pytest.param(
            ["production", "env:prod", "team:data"],
            {"production": "", "env": "prod", "team": "data"},
            id="mixed",
        ),
        pytest.param(["a:b:c"], {"a": "b:c"}, id="value-with-colon"),
        pytest.param(["env:"], {"env": ""}, id="trailing-colon"),
    ],
)
def test_expand_dag_tags(tag_names: list[str], expected: dict[str, str]) -> None:
    assert expand_dag_tags(tag_names) == expected


def test_expand_dag_tags_accepts_generator() -> None:
    assert expand_dag_tags(name for name in ["env:prod"]) == {"env": "prod"}


def test_expand_dag_tags_preserves_legacy_collision_behavior() -> None:
    assert expand_dag_tags(["team:data", "team:ml"]) == {"team": "ml"}
