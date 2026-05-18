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
# Note: Any AirflowException raised is expected to cause the TaskInstance
#       to be marked in an ERROR state
"""Exceptions used by Databricks Provider."""

from __future__ import annotations

from airflow.providers.common.compat.sdk import AirflowException


class DatabricksSqlExecutionError(AirflowException):
    """Raised when there is an error in sql execution."""


class DatabricksSqlExecutionTimeout(DatabricksSqlExecutionError):
    """Raised when a sql execution times out."""


class DatabricksWorkflowRepairError(AirflowException):
    """Raised when Databricks Workflow repair coordination fails."""


class DatabricksWorkflowRepairMetadataError(DatabricksWorkflowRepairError):
    """Raised when workflow repair metadata is missing or invalid."""


class DatabricksWorkflowRepairBudgetExhausted(DatabricksWorkflowRepairError):
    """Raised when a Databricks Workflow run fails after exhausting repair attempts."""


class DatabricksWorkflowRepairTriggerError(DatabricksWorkflowRepairError):
    """Raised when a Databricks Workflow repair trigger emits an invalid event."""
