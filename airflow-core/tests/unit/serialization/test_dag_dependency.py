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

from airflow.serialization.dag_dependency import DagDependency


class TestDagDependency:
    @pytest.mark.parametrize("dep_type", ("asset", "asset-alias", "asset-name-ref", "asset-uri-ref"))
    def test_node_id_with_asset(self, dep_type):
        dag_dep = DagDependency(
            source=dep_type,
            target="target",
            label="label",
            dependency_type=dep_type,
            dependency_id="id",
        )
        assert dag_dep.node_id == f"{dep_type}:id"

    def test_node_id(self):
        dag_dep = DagDependency(
            source="source",
            target="target",
            label="label",
            dependency_type="trigger",
            dependency_id="id",
        )
        assert dag_dep.node_id == "trigger:source:target:id"
