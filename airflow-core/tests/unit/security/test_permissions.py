#
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

from airflow.security import permissions


def test_resource_name_does_not_collide_with_reserved_resource_names():
    """A dag_id equal to a reserved resource name must be prefixed, not returned as-is.

    Regression for the case where a DAG literally named ``DAGs`` resolved to the
    global all-DAGs resource instead of its own ``DAG:DAGs`` resource.
    """
    # "DAGs" is the global resource name *and* a valid dag_id — it must resolve to
    # the per-DAG resource, never be returned unchanged.
    assert permissions.resource_name(permissions.RESOURCE_DAG, permissions.RESOURCE_DAG) == "DAG:DAGs"
    # Ordinary dag_ids are prefixed as before.
    assert permissions.resource_name("my_dag", permissions.RESOURCE_DAG) == "DAG:my_dag"
    # Already-prefixed names stay idempotent (the surviving short-circuit branch).
    assert permissions.resource_name("DAG:my_dag", permissions.RESOURCE_DAG) == "DAG:my_dag"
