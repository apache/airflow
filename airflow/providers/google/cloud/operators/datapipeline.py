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

from airflow import AirflowException
from airflow.providers.google.cloud.hooks.datapipeline import DEFAULT_DATAPIPELINE_LOCATION, DataPipelineHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator


class RunDataPipelineOperator(GoogleCloudBaseOperator):
    """
    Runs a Data Pipelines Instance using the Data Pipelines API.

    :param data_pipeline_name:  The display name of the pipeline. In example
        projects/PROJECT_ID/locations/LOCATION_ID/pipelines/PIPELINE_ID it would be the PIPELINE_ID.
    :param project_id: The ID of the GCP project that owns the job.
    :param location: The location of the Data Pipelines instance to (example_dags uses uscentral-1).
    :param gcp_conn_id: The connection ID to connect to the Google Cloud
        Platform.

    Returns the created Job in JSON representation.
    """

    def __init__(
        self,
        data_pipeline_name: str,
        project_id: str | None = None,
        location: str = DEFAULT_DATAPIPELINE_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.data_pipeline_name = data_pipeline_name
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Context):
        self.data_pipeline_hook = DataPipelineHook(gcp_conn_id=self.gcp_conn_id)

        if self.data_pipeline_name is None:
            raise AirflowException("Data Pipeline name not given; cannot run unspecified pipeline.")
        if self.project_id is None:
            raise AirflowException("Data Pipeline Project ID not given; cannot run pipeline.")
        if self.location is None:
            raise AirflowException("Data Pipeline location not given; cannot run pipeline.")

        self.response = self.data_pipeline_hook.run_data_pipeline(
            data_pipeline_name=self.data_pipeline_name,
            project_id=self.project_id,
            location=self.location,
        )

        if "error" in self.response:
            raise AirflowException(self.response.get("error").get("message"))

        return self.response
