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
#

import json

import requests

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class OpsgenieAlertHook(HttpHook):
    """
    This hook allows you to post alerts to Opsgenie.
    Accepts a connection that has an Opsgenie API key as the connection's password.
    This hook sets the domain to conn_id.host, and if not set will default
    to ``https://api.opsgenie.com``.

    Each Opsgenie API key can be pre-configured to a team integration.
    You can override these defaults in this hook.

    :param opsgenie_conn_id: The name of the Opsgenie connection to use
    :type opsgenie_conn_id: str

    """
    def __init__(self,
                 opsgenie_conn_id='opsgenie_default',
                 *args,
                 **kwargs
                 ):
        super().__init__(http_conn_id=opsgenie_conn_id, *args, **kwargs)

    def _get_api_key(self):
        """
        Get Opsgenie api_key for creating alert
        """
        conn = self.get_connection(self.http_conn_id)
        api_key = conn.password
        if not api_key:
            raise AirflowException('Opsgenie API Key is required for this hook, '
                                   'please check your conn_id configuration.')
        return api_key

    def get_conn(self, headers=None):
        """
        Overwrite HttpHook get_conn because this hook just needs base_url
        and headers, and does not need generic params

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """
        conn = self.get_connection(self.http_conn_id)
        self.base_url = conn.host if conn.host else 'https://api.opsgenie.com'
        session = requests.Session()
        if headers:
            session.headers.update(headers)
        return session

    def execute(self, payload=None):
        """
        Execute the Opsgenie Alert call

        :param payload: Opsgenie API Create Alert payload values
            See https://docs.opsgenie.com/docs/alert-api#section-create-alert
        :type payload: dict
        """
        payload = payload or {}
        api_key = self._get_api_key()
        return self.run(endpoint='v2/alerts',
                        data=json.dumps(payload),
                        headers={'Content-Type': 'application/json',
                                 'Authorization': 'GenieKey %s' % api_key})
