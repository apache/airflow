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

# pylint: disable=missing-function-docstring
"""
### TaskFlow API Tutorial Documentation

This is a simple ETL data pipeline example which demonstrates the use of the TaskFlow API
using three simple tasks for Extract, Transform, and Load.

Documentation that goes along with the Airflow TaskFlow API tutorial located
[here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
"""
# [START tutorial]
# [START import_module]
import json

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.dates import days_ago

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}
# [END default_args]

# [START instantiate_dag]
with DAG(
    'tutorial_taskflow_api_etl_dag',
    default_args=default_args,
    description='TaskFlow API ETL DAG tutorial',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    # [END instantiate_dag]

    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START extract]
    @dag.task()
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data pipeline.
        In this case, getting data is simulated by reading from a hardcoded JSON string.
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict

    # [END extract]

    # [START transform]
    @dag.task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and computes
        the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    # [END transform]

    # [START load]
    @dag.task()
    def load(total_order_value: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and instead of
        saving it to end user review, just prints it out.
        """

        print("Total order value is: %.2f" % total_order_value)

    # [END load]

    # [START main_flow]
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])
    # [END main_flow]


# [END tutorial]
