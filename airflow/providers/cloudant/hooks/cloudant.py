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
"""Hook for Cloudant"""
from typing import Dict

from cloudant import cloudant

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class CloudantHook(BaseHook):
    """
    Interact with Cloudant. This class is a thin wrapper around the cloudant python library.

    .. seealso:: the latest documentation `here <https://python-cloudant.readthedocs.io/en/latest/>`_.

    :param cloudant_conn_id: The connection id to authenticate and get a session object from cloudant.
    :type cloudant_conn_id: str
    """

    conn_name_attr = 'cloudant_conn_id'
    default_conn_name = 'cloudant_default'
    conn_type = 'cloudant'
    hook_name = 'Cloudant'

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['port', 'extra'],
            "relabeling": {'host': 'Account', 'login': 'Username (or API Key)', 'schema': 'Database'},
        }

    def __init__(self, cloudant_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.cloudant_conn_id = cloudant_conn_id

    def get_conn(self) -> cloudant:
        """
        Opens a connection to the cloudant service and closes it automatically if used as context manager.

        .. note::
            In the connection form:
            - 'host' equals the 'Account' (optional)
            - 'login' equals the 'Username (or API Key)' (required)
            - 'password' equals the 'Password' (required)

        :return: an authorized cloudant session context manager object.
        :rtype: cloudant
        """
        conn = self.get_connection(self.cloudant_conn_id)

        self._validate_connection(conn)

        cloudant_session = cloudant(user=conn.login, passwd=conn.password, account=conn.host)

        return cloudant_session

    def _validate_connection(self, conn: cloudant) -> None:
        for conn_param in ['login', 'password']:
            if not getattr(conn, conn_param):
                raise AirflowException(f'missing connection parameter {conn_param}')
