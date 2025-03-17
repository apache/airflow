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
    filtered_dagbag_stats = []
    allowed = True
    if readable_dags_filter.value:
        for each_file_load_stat in dagbag.dagbag_stats:
            dags_in_file = eval(each_file_load_stat.dags)
            for each in dags_in_file:
                if each in readable_dags_filter.value:
                    continue
                else:
                    allowed = False
            if allowed:
                filtered_dagbag_stats.append(each_file_load_stat)
            allowed = False

    return DagReportCollectionResponse(
        dag_reports=cast(list[DagReportResponse], filtered_dagbag_stats),
        total_entries=len(filtered_dagbag_stats),
    )
