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
"""This module contains Google BigQuery Data Transfer links."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.utils.context import Context

BIGQUERY_BASE_LINK = "/bigquery/transfers"
BIGQUERY_DTS_LINK = BIGQUERY_BASE_LINK + "/locations/{region}/configs/{config_id}/runs?project={project_id}"


class BigQueryDataTransferConfigLink(BaseGoogleLink):
    """Helper class for constructing BigQuery Data Transfer Config Link."""

    name = "BigQuery Data Transfer Config"
    key = "bigquery_dts_config"
    format_str = BIGQUERY_DTS_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance: BaseOperator,
        region: str,
        config_id: str,
        project_id: str,
    ):
        task_instance.xcom_push(
            context,
            key=BigQueryDataTransferConfigLink.key,
            value={"project_id": project_id, "region": region, "config_id": config_id},
        )
