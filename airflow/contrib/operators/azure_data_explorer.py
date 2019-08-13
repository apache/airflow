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
#
"""This module contains Azure Data Explorer operators"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.azure_data_explorer_hook import AzureDataExplorerHook


class AzureDataExplorerQueryOperator(BaseOperator):
    """
    Operator for querying Azure Data Explorer (Kusto).

    :param query: KQL query to run (templated).
    :type query: str
    :param database: Database to run the query on (templated).
    :type database: str
    :param options: Optional query options. See:
      https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties#list-of-clientrequestproperties
    :type options: dict
    :param azure_data_explorer_conn_id: Azure Data Explorer connection to use.
    :type azure_data_explorer_conn_id: str
    """

    ui_color = '#00a1f2'
    template_fields = ('query', 'database')
    template_ext = ('.kql', )

    @apply_defaults
    def __init__(self,
                 query,
                 database,
                 options=None,
                 azure_data_explorer_conn_id='azure_data_explorer_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.database = database
        self.options = options
        self.azure_data_explorer_conn_id = azure_data_explorer_conn_id
        self.hook = None

    def get_hook(self):
        """Returns new instance of AzureDataExplorerHook"""
        return AzureDataExplorerHook(self.azure_data_explorer_conn_id)

    def execute(self, context):
        """
        Run KQL Query on Azure Data Explorer (Kusto).
        Returns `PrimaryResult` of Query v2 HTTP response contents
        (https://kusto.azurewebsites.net/docs/api/rest/response2.html).
        """
        self.hook = self.get_hook()
        response = self.hook.run_query(self.query, self.database, self.options)
        return response.primary_results[0]
