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

import json
from typing import Optional, Dict, Any, Union

import yandexcloud

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class YandexCloudBaseHook(BaseHook):
    """
    A base hook for Yandex.Cloud related tasks.

    :param connection_id: The connection ID to use when fetching connection info.
    :type connection_id: str
    """

    def __init__(
        self,
        connection_id: Optional[str] = None,
        default_folder_id: Union[dict, bool, None] = None,
        default_public_ssh_key: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.connection_id = connection_id or 'yandexcloud_default'
        self.connection = self.get_connection(self.connection_id)
        self.extras = self.connection.extra_dejson
        credentials = self._get_credentials()
        self.sdk = yandexcloud.SDK(**credentials)
        self.default_folder_id = default_folder_id or self._get_field('folder_id', False)
        self.default_public_ssh_key = default_public_ssh_key or self._get_field('public_ssh_key', False)
        self.client = self.sdk.client

    def _get_credentials(self) -> Dict[str, Any]:
        service_account_json_path = self._get_field('service_account_json_path', False)
        service_account_json = self._get_field('service_account_json', False)
        oauth_token = self._get_field('oauth', False)
        if not (service_account_json or oauth_token or service_account_json_path):
            raise AirflowException(
                'No credentials are found in connection. Specify either service account '
                + 'authentication JSON or user OAuth token in Yandex.Cloud connection'
            )
        if service_account_json_path:
            with open(service_account_json_path) as infile:
                service_account_json = infile.read()
        if service_account_json:
            service_account_key = json.loads(service_account_json)
            return {'service_account_key': service_account_key}
        else:
            return {'token': oauth_token}

    def _get_field(self, field_name: str, default: Any = None) -> Any:
        """
        Fetches a field from extras, and returns it.
        """
        long_f = f'extra__yandexcloud__{field_name}'
        if hasattr(self, 'extras') and long_f in self.extras:
            return self.extras[long_f]
        else:
            return default
