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
"""
Example DAG to test Exasol connection in Apache Airflow.

This DAG uses the ExasolToS3Operator to execute a simple SQL query on Exasol. 
The main purpose is to verify that the Exasol connectionsettings (exasol_default) 
are working correctly.
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.transfers.exasol_to_s3 import ExasolExportOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

with DAG(
    dag_id="example_exasol_to_s3",
    default_args=default_args,
    start_date=datetime(2025, 3, 11),
    schedule=None,
    catchup=False,
    tags=["exasol", "test"],
) as dag:
    
    test_exasol_connection = ExasolExportOperator(
        task_id="test_exasol_conn",
        query_or_table="SELECT 1 AS val",
        export_params={},
        query_params={},
        exasol_conn_id="exasol_default",
    )

    test_exasol_connection