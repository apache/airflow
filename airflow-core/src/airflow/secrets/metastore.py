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
"""Objects relating to sourcing connections from metastore database."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import or_, select

from airflow.secrets import BaseSecretsBackend
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models import Connection


class MetastoreBackend(BaseSecretsBackend):
    """Retrieves Connection object and Variable from airflow metastore database."""

    @provide_session
    def get_connection(self, conn_id: str, session: Session = NEW_SESSION) -> Connection | None:
        """
        Get Airflow Connection from Metadata DB.

        :param conn_id: Connection ID
        :param session: SQLAlchemy Session
        :return: Connection Object
        """
        from airflow.models import Connection

        conn = session.scalar(select(Connection).where(Connection.conn_id == conn_id).limit(1))
        session.expunge_all()
        return conn

    @provide_session
    def get_variable(
        self, key: str, team_name: str | None = None, session: Session = NEW_SESSION
    ) -> str | None:
        """
        Get Airflow Variable from Metadata DB.

        :param key: Variable Key
        :param team_name: Team name associated to the task trying to access the variable (if any)
        :param session: SQLAlchemy Session
        :return: Variable Value
        """
        from airflow.models import Variable

        var_value = session.scalar(
            select(Variable)
            .where(Variable.key == key, or_(Variable.team_name == team_name, Variable.team_name.is_(None)))
            .limit(1)
        )
        session.expunge_all()
        if var_value:
            return var_value.val
        return None
