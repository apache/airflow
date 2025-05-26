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

from __future__ import annotations

import ast
import os
from typing import cast

from fastapi import Depends, HTTPException, status

from airflow import settings
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_report import (
    DagReportCollectionResponse,
    DagReportResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    ReadableDagsFilterDep,
    requires_access_dag,
)
from airflow.models.dagbag import DagBag

dag_report_router = AirflowRouter(tags=["DagReport"], prefix="/dagReports")


@dag_report_router.get(
    "",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET"))],
)
def get_dag_reports(
    subdir: str,
    readable_dags_filter: ReadableDagsFilterDep,
):
    """Get DAG report."""
    fullpath = os.path.normpath(subdir)
    if not fullpath.startswith(settings.DAGS_FOLDER):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "subdir should be subpath of DAGS_FOLDER settings")

    dagbag = DagBag(fullpath)

    readable_dag_ids: set[str] | None = readable_dags_filter.value
    if readable_dag_ids:
        filtered_dagbag_stats = [
            file_load_stat
            for file_load_stat in dagbag.dagbag_stats
            if len(set(ast.literal_eval(file_load_stat.dags)) - readable_dag_ids) == 0
        ]
    else:
        filtered_dagbag_stats = []

    return DagReportCollectionResponse(
        dag_reports=cast("list[DagReportResponse]", filtered_dagbag_stats),
        total_entries=len(filtered_dagbag_stats),
    )
