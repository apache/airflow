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
"""Secrets backends for task execution contexts."""

from __future__ import annotations

from airflow.sdk.execution_time.secrets.execution_api import ExecutionAPISecretsBackend

__all__ = ["ExecutionAPISecretsBackend", "DEFAULT_SECRETS_SEARCH_PATH_WORKERS"]

# Server-side default secrets search path (for comparison/detection only)
# This matches what airflow-core uses but is defined here to avoid importing from core
_SERVER_DEFAULT_SECRETS_SEARCH_PATH = [
    "airflow.secrets.environment_variables.EnvironmentVariablesBackend",
    "airflow.secrets.metastore.MetastoreBackend",
]

DEFAULT_SECRETS_SEARCH_PATH_WORKERS = [
    "airflow.secrets.environment_variables.EnvironmentVariablesBackend",
    "airflow.sdk.execution_time.secrets.execution_api.ExecutionAPISecretsBackend",
]
