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

from airflow import models
from airflow.providers.microsoft.azure.operators.adx import AzureDataExplorerQueryOperator
from airflow.utils.dates import days_ago


# Operator for querying Azure Data Explorer (Kusto).

# :param query: KQL query to run (templated).
# :type query: str
# :param database: Database to run the query on (templated).
# :type database: str
# :param options: Optional query options. See:
#     https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties#list-of-clientrequestproperties
# :type options: dict
# :param azure_data_explorer_conn_id: Reference to the
#     :ref:`Azure Data Explorer connection<howto/connection:adx>`.
# :type azure_data_explorer_conn_id: str



with models.DAG(
    "example_adx_query",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['example'],
) as dag:

    KQL_QUERY = """  """
    database_name = " "

    # [START azure_data_explorer_query_operator_howto_guide_create_table]

    # [END azure_data_explorer_query_operator_howto_guide_create_table]


    # [START azure_data_explorer_query_operator_howto_guide_query]

    # [END azure_data_explorer_query_operator_howto_guide_query]




    

    