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

import warnings
from typing import TYPE_CHECKING

from sqlalchemy import select

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.secrets import BaseSecretsBackend
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.connection import Connection


class MetastoreBackend(BaseSecretsBackend):
    """Retrieves Connection object and Variable from airflow metastore database."""

    @provide_session
    def get_connection(self, conn_id: str, session: Session = NEW_SESSION) -> Connection | None:
        return MetastoreBackend._fetch_connection(conn_id, session=session)

    @provide_session
    def get_connections(self, conn_id: str, session: Session = NEW_SESSION) -> list[Connection]:
        warnings.warn(
            "This method is deprecated. Please use "
            "`airflow.secrets.metastore.MetastoreBackend.get_connection`.",
            RemovedInAirflow3Warning,
            stacklevel=3,
        )
        conn = self.get_connection(conn_id=conn_id, session=session)
        if conn:
            return [conn]
        return []

    @provide_session
    def get_variable(self, key: str, session: Session = NEW_SESSION) -> str | None:
        """
        Get Airflow Variable from Metadata DB.

        :param key: Variable Key
        :return: Variable Value
        """
        return MetastoreBackend._fetch_variable(key=key, session=session)

    @staticmethod
    @internal_api_call
    @provide_session
    def _fetch_connection(conn_id: str, session: Session = NEW_SESSION) -> Connection | None:
        from airflow.models.connection import Connection

        conn = session.scalar(select(Connection).where(Connection.conn_id == conn_id).limit(1))
        session.expunge_all()
        return conn

    @staticmethod
    @internal_api_call
    @provide_session
    def _fetch_variable(key: str, session: Session = NEW_SESSION) -> str | None:
        from airflow.models.variable import Variable

        var_value = session.scalar(select(Variable).where(Variable.key == key).limit(1))
        session.expunge_all()
        if var_value:
            return var_value.val
        return None
