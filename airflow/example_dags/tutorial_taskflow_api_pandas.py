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

# [START tutorial]
# [START import_module]
import pandas as pd

# The DAG object; we'll need this to instantiate a DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago

# [END import_module]


def generate_data_file():
    tmp_data_file = open('/tmp/order_data.csv', 'w')
    tmp_data_file.write('order_id,order_value\n')
    tmp_data_file.write('"1001", 301.27\n')
    tmp_data_file.write('"1002", 433.21\n')
    tmp_data_file.write('"1003", 502.22\n')
    tmp_data_file.close()


# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}
# [END default_args]


# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2))
def tutorial_taskflow_api_pandas():
    """
    ### TaskFlow API Example using Pandas

    This is a simple ETL data pipeline example which demonstrates the use of the
    TaskFlow API using three simple tasks for Extract, Transform, and Load, while
    using Pandas and loading data from files.
    """
    # [END instantiate_dag]

    # [START extract]
    @task()
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, data is read from a file.
        """
        order_data_file = '/tmp/order_data.csv'

        order_data_df = pd.read_csv(order_data_file)

        # Make sure to set xcom configuration in the airflow.cfg file to
        # use the custom Xcom back-end
        # enable_xcom_pickling = True
        # this setting.
        #
        return order_data_df

        # If NOT using the custom Xcom back-end which is available with
        # this example ...
        # convert to JSON so that it be returned using xcom
        # order_data_df_str = order_data_df.to_json(orient='split')
        # return order_data_df_str

    # [END extract]

    # [START transform]
    @task(multiple_outputs=True)
    def transform(order_data_df: pd):
        """
        #### Transform task
        A simple Transform task which takes in the order data and computes
        the total order value.
        """
        # Alterntively, if using pickling and directly returning a pandas
        # dataframe, make sure to change the input signature to be a dataframe
        # instead of a string and skip the above converstion from a string
        # in JSON format to a pandas dataframe
        # order_data_df = pd.read_json(order_data_df_str, orient='split')

        total_series = order_data_df.sum(numeric_only=True)
        total_order_value = total_series.get('order_value')

        return {"total_order_value": total_order_value}

    # [END transform]

    # [START load]
    @task()
    def load(total_order_value: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print("Total order value is: %.2f" % total_order_value)

    # [END load]

    # [START main_flow]

    # in the example just generate the expected data file, so there are no
    # external dependencies needed to get the data file
    generate_data_file_task = PythonOperator(task_id='generate_data_file', python_callable=generate_data_file)

    # in the real world, ETL processing typically starts with a data file
    file_task = FileSensor(task_id='check_file', filepath='/tmp/order_data.csv')

    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])

    generate_data_file_task >> file_task >> order_data
    # [END main_flow]


# [START dag_invocation]
tutorial_etl_pandas_dag = tutorial_taskflow_api_pandas()
# [END dag_invocation]

# [END tutorial]
