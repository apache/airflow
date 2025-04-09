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

from airflow.serialization.dag_dependency import DagDependency


class TestDagDependency:
    def test_node_id(self):
        dag_dep = DagDependency(
            source="asset-name-ref",
            target="ref_consumer",
            label="abc",
            dependency_type="asset-name-ref",
            dependency_id="abc",
        )
        assert dag_dep.node_id == "asset-name-ref:abc"
