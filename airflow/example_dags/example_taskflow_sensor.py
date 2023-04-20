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
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.sensors.python import PythonSensor


default_args = {
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


def my_taskflow_sensor():
    # Check if the file exists OR you can give any logic
    if os.path.exists("/././path_to_file"):
        return True
    else:
        return False


with DAG(
    "example_dag",
    default_args=default_args,
    description="Example DAG with PythonSensor",
    schedule_interval=None,
    catchup=False,
) as dag:

    # [START example_taskflow_sensor]
    t1 = PythonSensor(
        task_id="my_python_sensor",
        python_callable=my_taskflow_sensor,
        poke_interval=60,  # Check every 60 seconds
        timeout=600,  # Timeout after 600 seconds (10 minutes)
    )
    # [END example_taskflow_sensor]


    t1
