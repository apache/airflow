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
from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import is_venv_installed

log = logging.getLogger(__name__)

if not is_venv_installed():
    log.warning(
        "The tutorial_taskflow_api_virtualenv example DAG requires virtualenv, please install it."
    )
else:

    @dag(schedule=None, start_date=datetime(2021, 1, 1), catchup=False, tags=["example"])
    def tutorial_taskflow_api_virtualenv():
        """
        ### TaskFlow API example using virtualenv
        This is a simple data pipeline example which demonstrates the use of
        the TaskFlow API using three simple tasks for Extract, Transform, and Load.
        """

        @task.virtualenv(
            serializer="dill",  # Use `dill` for advanced serialization.
            system_site_packages=False,
            requirements=["funcsigs"],
        )
        def extract():
            """
            #### Extract task
            A simple Extract task to get data ready for the rest of the data
            pipeline. In this case, getting data is simulated by reading from a
            hardcoded JSON string.
            """
            import json

            data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

            order_data_dict = json.loads(data_string)
            return order_data_dict

        @task(multiple_outputs=True)
        def transform(order_data_dict: dict):
            """
            #### Transform task
            A simple Transform task which takes in the collection of order data and
            computes the total order value.
            """
            total_order_value = 0

            for value in order_data_dict.values():
                total_order_value += value

            return {"total_order_value": total_order_value}

        @task()
        def load(total_order_value: float):
            """
            #### Load task
            A simple Load task which takes in the result of the Transform task and
            instead of saving it to end user review, just prints it out.
            """

            print(f"Total order value is: {total_order_value:.2f}")

        order_data = extract()
        order_summary = transform(order_data)
        load(order_summary["total_order_value"])

    tutorial_dag = tutorial_taskflow_api_virtualenv()
