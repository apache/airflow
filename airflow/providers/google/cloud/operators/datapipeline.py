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
"""This module contains Google Data Pipelines operators."""

from __future__ import annotations

from deprecated import deprecated

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.dataflow import DEFAULT_DATAFLOW_LOCATION
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowCreatePipelineOperator,
    DataflowRunPipelineOperator,
)
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID


@deprecated(
    reason="The CreateDataPipelineOperator is deprecated and will be removed after 01.12.2024. "
    "Please use DataflowCreatePipelineOperator class instead.",
    category=AirflowProviderDeprecationWarning,
)
class CreateDataPipelineOperator(DataflowCreatePipelineOperator):
    """Creates a new Data Pipelines instance from the Data Pipelines API."""


@deprecated(
    reason="The RunDataPipelineOperator is deprecated and will be removed after 01.12.2024. "
    "Please use DataflowRunPipelineOperator class instead.",
    category=AirflowProviderDeprecationWarning,
)
class RunDataPipelineOperator(DataflowRunPipelineOperator):
    """Runs a Data Pipelines Instance using the Data Pipelines API."""

    def __init__(
        self,
        data_pipeline_name: str,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(
            pipeline_name=data_pipeline_name,
            project_id=project_id,
            location=location,
            gcp_conn_id=gcp_conn_id,
            **kwargs,
        )
