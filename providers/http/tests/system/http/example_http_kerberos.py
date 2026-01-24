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

"""
Example DAG demonstrating the use of Kerberos authentication with HttpOperator.

This example shows how to configure Kerberos authentication for HTTP tasks
using Airflow connections, enabling secure communication with Kerberos-protected
services without manual ticket management.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator

# Example connection setup (typically done via Airflow UI or CLI):
# Connection ID: kerberized_api
# Connection Type: HTTP
# Host: api.example.com
# Extra: {
#   "kerberos_principal": "airflow@EXAMPLE.COM",
#   "kerberos_keytab": "/opt/airflow/keytabs/airflow.keytab"
# }

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_http_kerberos"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    tags=["example", "http", "kerberos"],
    catchup=False,
) as dag:
    # Example 1: Simple Kerberos-authenticated GET request
    get_kerberized_data = HttpOperator(
        task_id="get_kerberized_data",
        http_conn_id="kerberized_api",
        endpoint="api/v1/data",
        method="GET",
        log_response=True,
    )

    # Example 2: POST request with data to Kerberos-protected endpoint
    post_kerberized_data = HttpOperator(
        task_id="post_kerberized_data",
        http_conn_id="kerberized_api",
        endpoint="api/v1/submit",
        method="POST",
        data={"job_id": "12345", "status": "completed"},
        headers={"Content-Type": "application/json"},
        log_response=True,
    )

    # Example 3: Kerberos with custom ccache directory
    # Connection Extra: {
    #   "kerberos_principal": "airflow@EXAMPLE.COM",
    #   "kerberos_keytab": "/opt/airflow/keytabs/airflow.keytab",
    #   "kerberos_ccache_dir": "/tmp/krb5cc",
    #   "kerberos_forwardable": true
    # }

    get_data_custom_ccache = HttpOperator(
        task_id="get_data_custom_ccache",
        http_conn_id="kerberized_api_custom",
        endpoint="api/v1/secure/data",
        method="GET",
        log_response=True,
    )

    get_kerberized_data >> post_kerberized_data >> get_data_custom_ccache

# Example of using KerberosAuth directly in a custom operator or hook:
#
# from airflow.providers.http.auth import KerberosAuth
# from airflow.providers.http.hooks.http import HttpHook
#
# def my_custom_task():
#     # Create Kerberos auth from connection
#     hook = HttpHook(http_conn_id='kerberized_api', method='GET')
#
#     # The hook automatically handles Kerberos if configured in connection
#     response = hook.run(endpoint='api/data')
#     return response.json()
#
# # Or use KerberosAuth directly with requests:
#
# from airflow.providers.http.auth.kerberos import KerberosAuth
# import requests
#
# def my_kerberos_request():
#     auth = KerberosAuth(
#         principal="user@REALM",
#         keytab="/path/to/keytab"
#     )
#
#     with auth.ticket():
#         # Ticket is initialized here
#         session = requests.Session()
#         session.auth = auth.get_requests_auth()
#         response = session.get("https://kerberized-service.com/api")
#         return response.json()
#     # Ticket is automatically cleaned up here
