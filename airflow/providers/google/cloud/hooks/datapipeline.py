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

from typing import Sequence

from googleapiclient.discovery import build

from airflow.providers.google.common.hooks.base_google import (
    GoogleBaseHook,
)

DEFAULT_DATAPIPELINE_LOCATION = "us-central1"


class DataPipelineHook(GoogleBaseHook):
    """
    Hook for Google Data Pipelines.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )

    def get_conn(self) -> build:
        """Returns a Google Cloud Data Pipelines service object."""
        http_authorized = self._authorize()
        return build("datapipelines", "v1", http=http_authorized, cache_discovery=False)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_data_pipeline(
        self,
        body: dict,
        project_id: str,
        location: str = DEFAULT_DATAPIPELINE_LOCATION,
    ) -> None:
        """
        Creates a new Data Pipelines instance from the Data Pipelines API.

        :param body: The request body (contains instance of Pipeline). See:
            https://cloud.google.com/dataflow/docs/reference/data-pipelines/rest/v1/projects.locations.pipelines/create#request-body
        :param project_id: The ID of the GCP project that owns the job.
        :param location: The location to direct the Data Pipelines instance to (for example us-central1).

        Returns the created Data Pipelines instance in JSON representation.
        """
        parent = self.build_parent_name(project_id, location)
        service = self.get_conn()
        self.log.info(dir(service.projects().locations()))
        request = (
            service.projects()
            .locations()
            .pipelines()
            .create(
                parent=parent,
                body=body,
            )
        )
        response = request.execute(num_retries=self.num_retries)
        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def run_data_pipeline(
        self,
        data_pipeline_name: str,
        project_id: str,
        location: str = DEFAULT_DATAPIPELINE_LOCATION,
    ) -> None:
        """
        Runs a Data Pipelines Instance using the Data Pipelines API.

        :param data_pipeline_name:  The display name of the pipeline. In example
            projects/PROJECT_ID/locations/LOCATION_ID/pipelines/PIPELINE_ID it would be the PIPELINE_ID.
        :param project_id: The ID of the GCP project that owns the job.
        :param location: The location to direct the Data Pipelines instance to (for example us-central1).

        Returns the created Job in JSON representation.
        """
        parent = self.build_parent_name(project_id, location)
        service = self.get_conn()
        request = (
            service.projects()
            .locations()
            .pipelines()
            .run(
                name=f"{parent}/pipelines/{data_pipeline_name}",
                body={},
            )
        )
        response = request.execute(num_retries=self.num_retries)
        return response

    @staticmethod
    def build_parent_name(project_id: str, location: str):
        return f"projects/{project_id}/locations/{location}"
