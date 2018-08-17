# -*- coding: utf-8 -*-
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

from past.builtins import unicode

import cloudant

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class CloudantHook(BaseHook):
    """Interact with Cloudant.

    This class is a thin wrapper around the cloudant python library. See the
    documentation `here <https://github.com/cloudant-labs/cloudant-python>`_.
    """
    def __init__(self, cloudant_conn_id='cloudant_default'):
        super(CloudantHook, self).__init__('cloudant')
        self.cloudant_conn_id = cloudant_conn_id

    def get_conn(self):
        def _str(s):
            # cloudant-python doesn't support unicode.
            if isinstance(s, unicode):
                log = LoggingMixin().log
                log.debug(
                    'cloudant-python does not support unicode. Encoding %s as '
                    'ascii using "ignore".', s
                )
                return s.encode('ascii', 'ignore')

            return s

        conn = self.get_connection(self.cloudant_conn_id)

        for conn_param in ['host', 'password', 'schema']:
            if not hasattr(conn, conn_param) or not getattr(conn, conn_param):
                raise AirflowException(
                    'missing connection parameter {0}'.format(conn_param)
                )

        # In the connection form:
        # - 'host' is renamed to 'Account'
        # - 'login' is renamed 'Username (or API Key)'
        # - 'schema' is renamed to 'Database'
        #
        # So, use the 'host' attribute as the account name, and, if login is
        # defined, use that as the username.
        account = cloudant.Account(_str(conn.host))

        username = _str(conn.login or conn.host)

        account.login(
            username,
            _str(conn.password)).raise_for_status()

        return account.database(_str(conn.schema))

    def db(self):
        """Returns the Database object for this hook.

        See the documentation for cloudant-python here
        https://github.com/cloudant-labs/cloudant-python.
        """
        return self.get_conn()
