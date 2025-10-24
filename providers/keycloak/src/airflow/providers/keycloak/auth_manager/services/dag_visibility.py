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

"""
Utilities for resolving DAG visibility without storing allow-lists in Keycloak.

The current placeholder returns every DAG id registered in Airflow. Replace the logic
with your own mapping if finer-grained filtering is required.
"""

from __future__ import annotations

from airflow.models import DagModel
from airflow.utils.session import NEW_SESSION, provide_session


@provide_session
def resolve_allowed_dags(
    user_id: str,
    team_name: str | None = None,
    session=NEW_SESSION,
) -> set[str] | None:
    """
    Return the set of DAG ids Airflow should request access to on behalf of the user.

    :param user_id: Keycloak preferred username / user identifier.
    :param team_name: Optional team identifier coming from the UMA context.
    :return: Set of DAG ids the user should see.
    """
    dag_rows = session.query(DagModel.dag_id).all()
    return {dag_id for (dag_id,) in dag_rows}
