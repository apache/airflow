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

from contextlib import contextmanager
from typing import TYPE_CHECKING

from sqlalchemy import delete, select, update

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.orm import Session


@contextmanager
def attach_dag_to_team(session: Session, dag_id: str, *, bundle_name: str, team_name: str) -> Iterator[None]:
    """
    Associate a Dag with a team through a team-scoped bundle, for multi-team tests.

    On exit the Dag is moved back to the bundle it started in before the bundle and team
    created here are dropped, because ``DagModel.bundle_name`` is a foreign key with no
    ``ON DELETE`` action.

    :param session: session used for both the setup and the teardown writes
    :param dag_id: Dag to move under the team-scoped bundle
    :param bundle_name: name of the bundle to create; must not already exist
    :param team_name: name of the team to create and attach the bundle to
    """
    from airflow.models.dag import DagModel, clear_team_name_cache
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.team import Team

    original_bundle_name = session.scalar(select(DagModel.bundle_name).where(DagModel.dag_id == dag_id))
    bundle = DagBundleModel(name=bundle_name)
    bundle.teams.append(Team(name=team_name))
    session.add(bundle)
    session.flush()
    session.execute(update(DagModel).where(DagModel.dag_id == dag_id).values(bundle_name=bundle_name))
    session.commit()
    # DagModel.get_team_name caches by dag_id, so a lookup made before this re-association
    # would otherwise keep resolving to the Dag's previous team.
    clear_team_name_cache()
    try:
        yield
    finally:
        session.execute(
            update(DagModel).where(DagModel.dag_id == dag_id).values(bundle_name=original_bundle_name)
        )
        session.execute(delete(DagBundleModel).where(DagBundleModel.name == bundle_name))
        session.execute(delete(Team).where(Team.name == team_name))
        session.commit()
        clear_team_name_cache()
