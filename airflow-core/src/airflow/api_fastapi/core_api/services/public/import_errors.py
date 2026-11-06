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

from typing import TYPE_CHECKING

from sqlalchemy import select

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import AccessView
from airflow.models import DagModel
from airflow.models.dagbundle import DagBundleModel

if TYPE_CHECKING:
    from collections.abc import Collection

    from sqlalchemy.orm import Session

    from airflow.api_fastapi.auth.managers.models.base_user import BaseUser


def get_readable_import_error_files(
    bundle_name: str, filenames: Collection[str], *, user: BaseUser, session: Session
) -> set[str]:
    """
    Return the files of ``filenames`` whose import error ``user`` may read through ``GET /importErrors``.

    Mirrors that endpoint's rules: the caller needs ``AccessView.IMPORT_ERRORS``, and then either
    read access to at least one Dag registered from the file (stale ones included), or, for a file
    that registered no Dag, ``AccessView.IMPORT_ERRORS_ALL`` for the bundle's team. A file the
    caller can read only part of still counts, as the endpoint returns it with a redacted trace.
    """
    if not filenames:
        return set()
    auth_manager = get_auth_manager()
    if not auth_manager.is_authorized_view(access_view=AccessView.IMPORT_ERRORS, user=user):
        return set()

    dag_ids_by_file: dict[str, set[str]] = {}
    for relative_fileloc, dag_id in session.execute(
        select(DagModel.relative_fileloc, DagModel.dag_id).where(
            DagModel.bundle_name == bundle_name,
            DagModel.relative_fileloc.in_(filenames),
        )
    ):
        dag_ids_by_file.setdefault(relative_fileloc, set()).add(dag_id)

    readable: set[str] = set()
    if dag_ids_by_file:
        readable_dag_ids = auth_manager.get_authorized_dag_ids(method="GET", user=user)
        readable.update(
            filename for filename, dag_ids in dag_ids_by_file.items() if dag_ids & readable_dag_ids
        )

    unregistered = set(filenames) - dag_ids_by_file.keys()
    if unregistered and auth_manager.authorize_view(
        access_view=AccessView.IMPORT_ERRORS_ALL,
        user=user,
        team_name=DagBundleModel.get_team_name(bundle_name, session=session),
    ):
        readable.update(unregistered)
    return readable
