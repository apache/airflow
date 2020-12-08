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

from typing import Any

from azure.common.client_factory import get_client_from_auth_file, get_client_from_json_dict
from azure.common.credentials import ServicePrincipalCredentials

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class AzureBaseHook(BaseHook):
    """
    This hook acts as a base hook for azure services. It offers several authentication mechanisms to
    authenticate the client library used for upstream azure hooks.

    :param sdk_client: The SDKClient to use.
    :param conn_id: The azure connection id which refers to the information to connect to the service.
    """

    conn_name_attr = 'conn_id'
    default_conn_name = 'azure_default'
    conn_type = 'azure'
    hook_name = 'Azure'

    def __init__(self, sdk_client: Any, conn_id: str = 'azure_default'):
        self.sdk_client = sdk_client
        self.conn_id = conn_id
        super().__init__()

    def get_conn(self) -> Any:
        """
        Authenticates the resource using the connection id passed during init.

        :return: the authenticated client.
        """
        conn = self.get_connection(self.conn_id)

        key_path = conn.extra_dejson.get('key_path')
        if key_path:
            if not key_path.endswith('.json'):
                raise AirflowException('Unrecognised extension for key file.')
            self.log.info('Getting connection using a JSON key file.')
            return get_client_from_auth_file(client_class=self.sdk_client, auth_path=key_path)

        key_json = conn.extra_dejson.get('key_json')
        if key_json:
            self.log.info('Getting connection using a JSON config.')
            return get_client_from_json_dict(client_class=self.sdk_client, config_dict=key_json)

        self.log.info('Getting connection using specific credentials and subscription_id.')
        return self.sdk_client(
            credentials=ServicePrincipalCredentials(
                client_id=conn.login, secret=conn.password, tenant=conn.extra_dejson.get('tenantId')
            ),
            subscription_id=conn.extra_dejson.get('subscriptionId'),
        )
