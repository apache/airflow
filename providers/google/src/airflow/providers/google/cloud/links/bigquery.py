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
"""This module contains Google BigQuery links."""

from __future__ import annotations

from airflow.providers.google.cloud.links.base import BaseGoogleLink

BIGQUERY_BASE_LINK = "/bigquery"
BIGQUERY_DATASET_LINK = (
    BIGQUERY_BASE_LINK + "?referrer=search&project={project_id}&d={dataset_id}&p={project_id}&page=dataset"
)
BIGQUERY_TABLE_LINK = (
    BIGQUERY_BASE_LINK
    + "?referrer=search&project={project_id}&d={dataset_id}&p={project_id}&page=table&t={table_id}"
)
BIGQUERY_JOB_DETAIL_LINK = (
    BIGQUERY_BASE_LINK + "?project={project_id}&ws=!1m5!1m4!1m3!1s{project_id}!2s{job_id}!3s{location}"
)


class BigQueryDatasetLink(BaseGoogleLink):
    """Helper class for constructing BigQuery Dataset Link."""

    name = "BigQuery Dataset"
    key = "bigquery_dataset"
    format_str = BIGQUERY_DATASET_LINK


class BigQueryTableLink(BaseGoogleLink):
    """Helper class for constructing BigQuery Table Link."""

    name = "BigQuery Table"
    key = "bigquery_table"
    format_str = BIGQUERY_TABLE_LINK


class BigQueryJobDetailLink(BaseGoogleLink):
    """Helper class for constructing BigQuery Job Detail Link."""

    name = "BigQuery Job Detail"
    key = "bigquery_job_detail"
    format_str = BIGQUERY_JOB_DETAIL_LINK
