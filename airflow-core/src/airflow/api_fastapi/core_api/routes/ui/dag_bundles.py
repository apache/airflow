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
from sqlalchemy import exists, select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.dag_bundles import DagBundleCountResponse
from airflow.api_fastapi.core_api.security import (
    AccessView,
    AuthManagerDep,
    GetUserDep,
    ReadableDagsFilterDep,
    requires_access_dag,
)
from airflow.models import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.errors import ParseImportError

dag_bundles_router = AirflowRouter(tags=["Dag Bundle"], prefix="/dagBundles")


@dag_bundles_router.get("/count", dependencies=[Depends(requires_access_dag(method="GET"))])
def get_dag_bundle_count(
    readable_dags_filter: ReadableDagsFilterDep,
    user: GetUserDep,
    auth_manager: AuthManagerDep,
    session: SessionDep,
) -> DagBundleCountResponse:
    """
    Count the Dag bundles holding a Dag or an import error the caller may read.

    Readable by anyone who can read Dags, unlike ``GET /dagBundles``, so the UI can tell whether a
    bundle name is worth showing next to an import error. Stale Dags count, and so do import errors
    in files that never registered a Dag, which ``GET /importErrors`` shows to holders of
    ``AccessView.IMPORT_ERRORS_ALL``: a bundle whose files all fail to import is where the name
    matters most.
    """
    bundle_names = set(session.scalars(readable_dags_filter.to_orm(select(DagModel.bundle_name).distinct())))
    dagless_error_bundle_names: set[str] = {
        bundle_name
        for bundle_name in session.scalars(
            select(ParseImportError.bundle_name)
            .distinct()
            .where(
                ParseImportError.bundle_name.is_not(None),
                ~exists().where(
                    DagModel.bundle_name == ParseImportError.bundle_name,
                    DagModel.relative_fileloc == ParseImportError.filename,
                ),
            )
        )
        if bundle_name is not None and bundle_name not in bundle_names
    }
    team_name_by_bundle = DagBundleModel.get_team_names(dagless_error_bundle_names, session=session)
    bundle_names.update(
        bundle_name
        for bundle_name in dagless_error_bundle_names
        if auth_manager.authorize_view(
            access_view=AccessView.IMPORT_ERRORS_ALL,
            user=user,
            team_name=team_name_by_bundle.get(bundle_name),
        )
    )
    return DagBundleCountResponse(total_entries=len(bundle_names))
