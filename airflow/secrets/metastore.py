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
"""Objects relating to sourcing connections from metastore database"""

from typing import TYPE_CHECKING, List

from airflow.secrets import BaseSecretsBackend
from airflow.utils.session import provide_session

if TYPE_CHECKING:
    from airflow.models.connection import Connection


class MetastoreBackend(BaseSecretsBackend):
    """Retrieves Connection object from airflow metastore database."""

    # pylint: disable=missing-docstring
    @provide_session
    def get_connections(self, conn_id, session=None) -> List['Connection']:
        from airflow.models.connection import Connection
        conn_list = session.query(Connection).filter(Connection.conn_id == conn_id).all()
        session.expunge_all()
        return conn_list

    @provide_session
    def get_variable(self, key: str, session=None):
        """
        Get Airflow Variable from Metadata DB

        :param key: Variable Key
        :return: Variable Value
        """
        from airflow.models.variable import Variable
        var_value = session.query(Variable).filter(Variable.key == key).first()
        session.expunge_all()
        if var_value:
            return var_value.val
        return None
