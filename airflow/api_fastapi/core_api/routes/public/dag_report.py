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

from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_report import (
    DagReportCollectionResponse,
    DagReportResponse,
)
from airflow.models.dagbag import DagBag
from airflow.utils.cli import process_subdir

dag_report_router = AirflowRouter(tags=["DagReport"], prefix="/dagReport")


@dag_report_router.get(
    "",
)
def get_dag_report(
    subdir: str,
):
    """Get DAG report."""
    # though the `subdir` will be validated on CLI side, we still need to validate again here or the CodeQL will report a security issue
    dagbag = DagBag(process_subdir(subdir))
    return DagReportCollectionResponse(
        dag_reports=[
            DagReportResponse.model_validate(
                dag_report,
                from_attributes=True,
            )
            for dag_report in dagbag.dagbag_stats
        ],
        total_entries=len(dagbag.dagbag_stats),
    )
