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

#
# Licensed to the Apache Software Foundation (ASF) under one
# ... (full ASF header, copy verbatim from any file in the repo)
# under the License.
# Note: Any AirflowException raised is expected to cause the TaskInstance
#       to be marked in an ERROR state
"""Exceptions used by Microsoft Azure Provider."""

from __future__ import annotations

from airflow.providers.common.compat.sdk import AirflowException


class AzureBatchVmPublisherMissingError(AirflowException):
    """Raised when vm_publisher is not provided."""


class AzureBatchLatestImageSpecIncompleteError(AirflowException):
    """Raised when use_latest_verified_vm_image_and_sku is requested without a complete image spec."""


class AzureBatchVmImageSpecIncompleteError(AirflowException):
    """Raised when vm_publisher is provided without vm_sku, vm_offer and vm_node_agent_sku_id."""


class AzureBatchPoolSizingMissingError(AirflowException):
    """Raised when neither target_dedicated_nodes nor enable_auto_scale is provided."""


class AzureBatchPoolSizingConflictError(AirflowException):
    """Raised when enable_auto_scale is combined with explicit node counts."""


class AzureBatchAutoScaleFormulaMissingError(AirflowException):
    """Raised when enable_auto_scale is set without auto_scale_formula."""


class AzureBatchJobPreparationTaskMissingError(AirflowException):
    """Raised when batch_job_release_task is provided without batch_job_preparation_task."""


class AzureBatchRequiredParametersMissingError(AirflowException):
    """Raised when one or more required pool, job or task parameters are missing."""
