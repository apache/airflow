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

import os.path
import shutil
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.delta.sharing.operators.delta_sharing import DeltaSharingLocalDownloadOperator
from airflow.providers.delta.sharing.sensors.delta_sharing import DeltaSharingSensor
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'airflow',
    'profile_file': 'https://github.com/delta-io/delta-sharing/raw/main/examples/open-datasets.share',
}

with DAG(
    'delta_sensor_boston_housing',
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    default_args=default_args,
    catchup=False,
    tags=["example", "delta-sharing"],
) as dag:
    share_name = "delta_sharing"
    schema_name = "default"
    table_name = "boston-housing"
    data_location = '/tmp/boston_housing'

    # [START howto_delta_sharing_sensor]
    # Example of using Delta Sharing Sensor to wait for changes in the table.
    check_boston_housing = DeltaSharingSensor(
        task_id='check_boston_housing',
        share=share_name,
        schema=schema_name,
        table=table_name,
        timeout=60,
    )
    # [END howto_delta_sharing_sensor]

    # [START howto_delta_sharing_operator]
    # Example of using DeltaSharingLocalDownloadOperator to download Delta Sharing table
    download_boston_housing = DeltaSharingLocalDownloadOperator(
        task_id='download_boston_housing',
        share=share_name,
        schema=schema_name,
        table=table_name,
        timeout_seconds=60,
        limit=100,
        location=data_location,
    )
    # [END howto_delta_sharing_operator]

    def do_data_check():
        if not os.path.exists(data_location) or not os.path.isdir(data_location):
            raise RuntimeError(f"{data_location} doesn't exist or not directory")
        dir_name = os.path.join(data_location, "_metadata")
        if not os.path.exists(dir_name) or not os.path.isdir(dir_name):
            raise RuntimeError(f"{dir_name} doesn't exist or not directory")
        dir_name = os.path.join(data_location, "_stats")
        if not os.path.exists(dir_name) or not os.path.isdir(dir_name):
            raise RuntimeError(f"{dir_name} doesn't exist or not directory")
        parquet_files = [file for file in os.listdir(data_location) if file.endswith(".parquet")]
        if len(parquet_files) == 0:
            raise RuntimeError("No data files were downloaded!")

        return True

    perform_data_check = PythonOperator(
        task_id="perform_data_check",
        python_callable=do_data_check,
    )

    def cleanup_downloaded_data():
        shutil.rmtree(data_location, ignore_errors=True)

    delete_downloaded_data = PythonOperator(
        task_id="cleanup_downloaded_data",
        python_callable=cleanup_downloaded_data,
    )
    delete_downloaded_data.trigger_rule = TriggerRule.ALL_DONE

    (
        # TEST BODY
        check_boston_housing
        >> download_boston_housing
        # Do data check
        >> perform_data_check
        # TEST TEARDOWN
        >> delete_downloaded_data
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
