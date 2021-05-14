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
from typing import Any, Dict, Optional

import httpx

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

    def __init__(self, opsgenie_conn_id: str = 'opsgenie_default', *args, **kwargs) -> None:
        super().__init__(http_conn_id=opsgenie_conn_id, *args, **kwargs)  # type: ignore[misc]

    def _get_api_key(self) -> str:
        """Get Opsgenie api_key for creating alert"""
        conn = self.get_connection(self.http_conn_id)
        api_key = conn.password
        if not api_key:
            raise AirflowException(
                'Opsgenie API Key is required for this hook, please check your conn_id configuration.'
            )
        return api_key

    def get_conn(
        self, headers: Optional[Dict[Any, Any]] = None, verify: bool = True, proxies=None, cert=None
    ) -> httpx.Client:
        """
        Overwrite HttpHook get_conn because this hook just needs base_url
        and headers, and does not need generic params

        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        :param verify: whether to verify SSL during the connection (only use for testing)
        :param proxies: A dictionary mapping proxy keys to proxy
        :param cert: client An SSL certificate used by the requested host
            to authenticate the client. Either a path to an SSL certificate file, or
            two-tuple of (certificate file, key file), or a three-tuple of (certificate
            file, key file, password).
        """
        client = super().get_conn(headers=headers, verify=verify, proxies=proxies, cert=cert)
        self.base_url = self.conn.host if self.conn.host else 'https://api.opsgenie.com'
        return client

    def execute(self, payload: Optional[dict] = None) -> Any:
        """
        Execute the Opsgenie Alert call

        :param payload: Opsgenie API Create Alert payload values
            See https://docs.opsgenie.com/docs/alert-api#section-create-alert
        :type payload: dict
        """
        payload = payload or {}
        api_key = self._get_api_key()
        return self.run(
            endpoint='v2/alerts',
            data=json.dumps(payload),
            headers={'Content-Type': 'application/json', 'Authorization': f'GenieKey {api_key}'},
        )
