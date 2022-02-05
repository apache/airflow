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
import os
from typing import Any, Optional, Tuple

from zenpy import Zenpy
from zenpy.lib.api import BaseApi
from zenpy.lib.api_objects import BaseObject

from airflow.hooks.base import BaseHook


class ZendeskHook(BaseHook):
    """
    Interact with Zendesk. This hook uses the Zendesk conn_id.

    :param zendesk_conn_id: The Airflow connection used for Zendesk credentials.
    """

    conn_name_attr = 'zendesk_conn_id'
    default_conn_name = 'zendesk_default'
    conn_type = 'zendesk'
    hook_name = 'Zendesk'

    def __init__(self, zendesk_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.zendesk_conn_id = zendesk_conn_id
        self.base_api: Optional[BaseApi] = None
        zenpy_client, url = self._init_conn()
        self.zenpy_client = zenpy_client
        self.__url = url
        # Keep reference to the _get method to allow arbitrary endpoint call for
        # backwards compatibility. (ZendeskHook.call method)
        self._get = self.zenpy_client.users._get

    def _init_conn(self) -> Tuple[Zenpy, str]:
        """
        Create the Zenpy Client for our Zendesk connection.

        :return: zenpy.Zenpy client and the url for the API.
        """
        conn = self.get_connection(self.zendesk_conn_id)
        url = "https://" + conn.host
        domain = conn.host
        subdomain: Optional[str] = None
        if conn.host.count(".") >= 2:
            dot_splitted_string = conn.host.rsplit(".", 2)
            subdomain = dot_splitted_string[0]
            domain = ".".join(dot_splitted_string[1:])
        return Zenpy(domain=domain, subdomain=subdomain, email=conn.login, password=conn.password), url

    def get_conn(self) -> Zenpy:
        """
        Get the underlying Zenpy client.

        :return: zenpy.Zenpy client.
        """
        return self.zenpy_client

    def call(
        self,
        path: str,
        query: Optional[dict] = None,
        **kwargs: Any,
    ) -> BaseObject:
        """
        Call Zendesk API and return results.

        This endpoint is kept for backward compatibility purpose but it should be
        removed in the future to expose specific methods for each resource.
        TODO: When removing this method, remove the reference to _get in __init__

        :param path: The Zendesk API to call.
        :param query: Query parameters.
        :param kwargs: (optional) Additional parameters directly passed to the
            request.Session.get method.
        :return: A BaseObject representing the response from Zendesk. You can call
            ``.to_dict()`` if you need to send it to XCom.
        """
        return self._get(url=os.path.join(self.__url, path.lstrip("/")), params=query, **kwargs)
