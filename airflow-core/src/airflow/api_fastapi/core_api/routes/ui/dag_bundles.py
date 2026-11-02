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

from fastapi import Depends
from sqlalchemy import func, select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.dag_bundles import DagBundleCountResponse
from airflow.api_fastapi.core_api.security import ReadableDagsFilterDep, requires_access_dag
from airflow.models import DagModel

dag_bundles_router = AirflowRouter(tags=["Dag Bundle"], prefix="/dagBundles")


@dag_bundles_router.get("/count", dependencies=[Depends(requires_access_dag(method="GET"))])
def get_dag_bundle_count(
    readable_dags_filter: ReadableDagsFilterDep,
    session: SessionDep,
) -> DagBundleCountResponse:
    """
    Count the Dag bundles holding a Dag the caller may read.

    Readable by anyone who can read Dags, unlike ``GET /dagBundles``, so the UI can tell whether a
    bundle name is worth showing next to an import error. A bundle holding only stale Dags is
    counted too: that is a bundle whose every file fails to import, where the name matters most.
    """
    count = session.scalar(readable_dags_filter.to_orm(select(func.count(DagModel.bundle_name.distinct()))))
    return DagBundleCountResponse(total_entries=count or 0)
