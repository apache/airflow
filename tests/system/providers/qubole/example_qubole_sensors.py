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

import os
import textwrap
from datetime import datetime

from airflow import DAG
from airflow.providers.qubole.sensors.qubole import QuboleFileSensor, QubolePartitionSensor

START_DATE = datetime(2021, 1, 1)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_qubole_sensor"


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=START_DATE,
    tags=['example'],
) as dag:
    dag.doc_md = textwrap.dedent(
        """
        This is only an example DAG to highlight usage of QuboleSensor in various scenarios,
        some of these tasks may or may not work based on your QDS account setup.

        Run a shell command from Qubole Analyze against your Airflow cluster with following to
        trigger it manually `airflow dags trigger example_qubole_sensor`.

        *Note: Make sure that connection `qubole_default` is properly set before running
        this example.*
        """
    )

    # [START howto_sensor_qubole_run_file_sensor]
    check_s3_file = QuboleFileSensor(
        task_id='check_s3_file',
        poke_interval=60,
        timeout=600,
        data={
            "files": [
                "s3://paid-qubole/HadoopAPIExamples/jars/hadoop-0.20.1-dev-streaming.jar",
                "s3://paid-qubole/HadoopAPITests/data/{{ ds.split('-')[2] }}.tsv",
            ]  # will check for availability of all the files in array
        },
    )
    # [END howto_sensor_qubole_run_file_sensor]

    # [START howto_sensor_qubole_run_partition_sensor]
    check_hive_partition = QubolePartitionSensor(
        task_id='check_hive_partition',
        poke_interval=10,
        timeout=60,
        data={
            "schema": "default",
            "table": "my_partitioned_table",
            "columns": [
                {"column": "month", "values": ["{{ ds.split('-')[1] }}"]},
                {"column": "day", "values": ["{{ ds.split('-')[2] }}", "{{ yesterday_ds.split('-')[2] }}"]},
            ],  # will check for partitions like [month=12/day=12,month=12/day=13]
        },
    )
    # [END howto_sensor_qubole_run_partition_sensor]

    check_s3_file >> check_hive_partition

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
