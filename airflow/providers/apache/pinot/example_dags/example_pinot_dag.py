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

"""Example DAG demonstrating the usage of the PinotAdminHook and PinotDbApiHook."""

from datetime import datetime
from airflow import DAG
from airflow.providers.apache.pinot.hooks.pinot import PinotAdminHook, PinotDbApiHook
from airflow.utils.dates import days_ago

with DAG(
    dag_id='example_pinot_hook',
    schedule_interval=None,
    start_date=datetime(),
    tags=['example'],
) as dag:
    # [START howto_operator_pinot_admin_hook]
    run_this = PinotAdminHook(
        task_id="run_example_pinot_script",
        pinot="ls /;",
        pinot_options="-x local",
    )
    # [END howto_operator_pinot_admin_hook]
    # [START howto_operator_pinot_dbapi_example]
    run_this = PinotDbApiHook(
        task_id="run_example_pinot_script",
        pinot="ls /;",
        pinot_options="-x local",
       
    )
    # [END howto_operator_pinot_dbapi_example]
