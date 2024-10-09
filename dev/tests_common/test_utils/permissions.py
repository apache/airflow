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


def _resource_name(dag_id: str, resource_name: str) -> str:
    """
    This method is to keep compatibility with new FAB versions
    running with old airflow versions.
    """
    if hasattr(permissions, "resource_name"):
        return getattr(permissions, "resource_name")(dag_id, resource_name)
    if resource_name == permissions.RESOURCE_DAG:
        return getattr(permissions, "resource_name_for_dag")(dag_id)
    raise Exception("Only DAG resource is supported in this Airflow version.")
