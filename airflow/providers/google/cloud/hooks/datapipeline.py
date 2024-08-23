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
"""This module contains a Google Data Pipelines Hook."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.dataflow import DataflowHook
from airflow.providers.google.common.deprecated import deprecated
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from googleapiclient.discovery import build

DEFAULT_DATAPIPELINE_LOCATION = "us-central1"


@deprecated(
    planned_removal_date="December 01, 2024",
    use_instead="DataflowHook",
    category=AirflowProviderDeprecationWarning,
)
class DataPipelineHook(DataflowHook):
    """Hook for Google Data Pipelines."""

    def get_conn(self) -> build:
        """Return a Google Cloud Data Pipelines service object."""
        return super().get_pipelines_conn()

    @GoogleBaseHook.fallback_to_default_project_id
    def create_data_pipeline(
        self,
        body: dict,
        project_id: str,
        location: str = DEFAULT_DATAPIPELINE_LOCATION,
    ) -> dict:
        """Create a new Data Pipelines instance from the Data Pipelines API."""
        return super().create_data_pipeline(body=body, project_id=project_id, location=location)

    @GoogleBaseHook.fallback_to_default_project_id
    def run_data_pipeline(
        self,
        data_pipeline_name: str,
        project_id: str,
        location: str = DEFAULT_DATAPIPELINE_LOCATION,
    ) -> dict:
        """Run a Data Pipelines Instance using the Data Pipelines API."""
        return super().run_data_pipeline(
            pipeline_name=data_pipeline_name, project_id=project_id, location=location
        )

    @staticmethod
    def build_parent_name(project_id: str, location: str):
        return f"projects/{project_id}/locations/{location}"
