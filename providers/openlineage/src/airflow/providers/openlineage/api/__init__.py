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
"""
Public API module - stable interface.

This module is part of the **public interface** of the `airflow.providers.openlineage` provider.

Functions, classes, and objects defined here are **intended for external use**, especially for dag authors
and other providers that want to emit custom OpenLineage events tied to the currently running Airflow task.

External code should depend **only on modules within** `airflow.providers.openlineage.api.*`.
"""

from __future__ import annotations

from airflow.providers.openlineage.api.core import emit, is_openlineage_active
from airflow.providers.openlineage.api.datasets import emit_dataset_lineage
from airflow.providers.openlineage.api.sql import emit_query_lineage

__all__ = [
    "emit",
    "emit_dataset_lineage",
    "emit_query_lineage",
    "is_openlineage_active",
]
