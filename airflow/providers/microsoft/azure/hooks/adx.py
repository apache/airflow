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

"""This module contains Azure Data Explorer hook"""
from typing import Dict, Optional

from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.request import ClientRequestProperties, KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.response import KustoResponseDataSetV2

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class AzureDataExplorerHook(BaseHook):
    """
    Interacts with Azure Data Explorer (Kusto).

    Extra JSON field contains the following parameters:

    .. code-block:: json

        {
            "tenant": "<Tenant ID>",
            "auth_method": "<Authentication method>",
            "certificate": "<Application PEM certificate>",
            "thumbprint": "<Application certificate thumbprint>"
        }

    **Cluster**:

    Azure Data Explorer cluster is specified by a URL, for example: "https://help.kusto.windows.net".
    The parameter must be provided through `Host` connection detail.

    **Tenant ID**:

    To learn about tenants refer to: https://docs.microsoft.com/en-us/onedrive/find-your-office-365-tenant-id

    **Authentication methods**:

    Authentication method must be provided through "auth_method" extra parameter.
    Available authentication methods are:

      - AAD_APP : Authentication with AAD application certificate. Extra parameters:
                  "tenant" is required when using this method. Provide application ID
                  and application key through username and password parameters.

      - AAD_APP_CERT: Authentication with AAD application certificate. Extra parameters:
                      "tenant", "certificate" and "thumbprint" are required
                      when using this method.

      - AAD_CREDS : Authentication with AAD username and password. Extra parameters:
                    "tenant" is required when using this method. Username and password
                    parameters are used for authentication with AAD.

      - AAD_DEVICE : Authenticate with AAD device code. Please note that if you choose
                     this option, you'll need to authenticate for every new instance
                     that is initialized. It is highly recommended to create one instance
                     and use it for all queries.

    :param azure_data_explorer_conn_id: Reference to the Azure Data Explorer connection.
    :type azure_data_explorer_conn_id: str
    """

    def __init__(self, azure_data_explorer_conn_id: str = 'azure_data_explorer_default') -> None:
        super().__init__()
        self.conn_id = azure_data_explorer_conn_id
        self.connection = self.get_conn()

    def get_conn(self) -> KustoClient:
        """Return a KustoClient object."""
        conn = self.get_connection(self.conn_id)
        cluster = conn.host
        if not cluster:
            raise AirflowException('Host connection option is required')

        def get_required_param(name: str) -> str:
            """Extract required parameter from extra JSON, raise exception if not found"""
            value = conn.extra_dejson.get(name)
            if not value:
                raise AirflowException(
                    'Extra connection option is missing required parameter: `{}`'.format(name)
                )
            return value

        auth_method = get_required_param('auth_method')

        if auth_method == 'AAD_APP':
            kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                cluster, conn.login, conn.password, get_required_param('tenant')
            )
        elif auth_method == 'AAD_APP_CERT':
            kcsb = KustoConnectionStringBuilder.with_aad_application_certificate_authentication(
                cluster,
                conn.login,
                get_required_param('certificate'),
                get_required_param('thumbprint'),
                get_required_param('tenant'),
            )
        elif auth_method == 'AAD_CREDS':
            kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication(
                cluster, conn.login, conn.password, get_required_param('tenant')
            )
        elif auth_method == 'AAD_DEVICE':
            kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(cluster)
        else:
            raise AirflowException('Unknown authentication method: {}'.format(auth_method))

        return KustoClient(kcsb)

    def run_query(self, query: str, database: str, options: Optional[Dict] = None) -> KustoResponseDataSetV2:
        """
        Run KQL query using provided configuration, and return
        `azure.kusto.data.response.KustoResponseDataSet` instance.
        If query is unsuccessful AirflowException is raised.

        :param query: KQL query to run
        :type query: str
        :param database: Database to run the query on.
        :type database: str
        :param options: Optional query options. See:
           https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties#list-of-clientrequestproperties
        :type options: dict
        :return: dict
        """
        properties = ClientRequestProperties()
        if options:
            for k, v in options.items():
                properties.set_option(k, v)
        try:
            return self.connection.execute(database, query, properties=properties)
        except KustoServiceError as error:
            raise AirflowException('Error running Kusto query: {}'.format(error))
