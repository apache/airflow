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
from datetime import datetime

from airflow.decorators import task
from airflow.models import DAG
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.ssh.operators.ssh import SSHOperator

SFTP_DIRECTORY = os.environ.get("SFTP_DIRECTORY", "example-empty-directory").rstrip("/") + "/"
FULL_FILE_PATH = f"{SFTP_DIRECTORY}example_test_sftp_sensor_decory_file.txt"
SFTP_DEFAULT_CONNECTION = "sftp_default"


@task.python
def sleep_function():
    import time

    time.sleep(60)


with DAG(
    "example_sftp_sensor",
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "sftp"],
) as dag:
    # [START howto_operator_sftp_sensor_decorator]
    @task.sftp_sensor(  # type: ignore[attr-defined]
        task_id="sftp_sensor",  # type: ignore[attr-defined]
        path=FULL_FILE_PATH,
        poke_interval=10,
    )
    def sftp_sensor_decorator():
        print("Files were successfully found!")
        # add your logic
        return "done"

    # [END howto_operator_sftp_sensor_decorator]

    remove_file_task_start = SSHOperator(
        task_id="remove_file_start",
        command=f"rm {FULL_FILE_PATH} || true",
        ssh_conn_id=SFTP_DEFAULT_CONNECTION,
    )
    remove_file_task_end = SSHOperator(
        task_id="remove_file_end", command=f"rm {FULL_FILE_PATH} || true", ssh_conn_id=SFTP_DEFAULT_CONNECTION
    )
    create_decoy_file_task = SSHOperator(
        task_id="create_file", command=f"touch {FULL_FILE_PATH}", ssh_conn_id=SFTP_DEFAULT_CONNECTION
    )
    sleep_task = sleep_function()
    sftp_with_sensor = sftp_sensor_decorator()

    # [START howto_operator_sftp_sensor]
    sftp_with_operator = SFTPSensor(task_id="sftp_operator", path=FULL_FILE_PATH, poke_interval=10)
    # [END howto_operator_sftp_sensor]

    # [START howto_sensor_sftp_deferrable]
    sftp_sensor_with_async = SFTPSensor(
        task_id="sftp_operator_async", path=FULL_FILE_PATH, poke_interval=10, deferrable=True
    )
    # [END howto_sensor_sftp_deferrable]

    remove_file_task_start >> sleep_task >> create_decoy_file_task
    (
        remove_file_task_start
        >> [sftp_with_operator, sftp_sensor_with_async, sftp_with_sensor]
        >> remove_file_task_end
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
