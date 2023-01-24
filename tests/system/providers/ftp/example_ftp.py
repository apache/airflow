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
This is an example dag for using the FTPFileTransmitOperator and FTPSFileTransmitOperator.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.ftp.operators.ftp import (
    FTPFileTransmitOperator,
    FTPOperation,
    FTPSFileTransmitOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_ftp_ftps_put_get"

with DAG(
    DAG_ID,
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "Ftp", "FtpFileTransmit", "Ftps", "FtpsFileTransmit"],
) as dag:
    # [START howto_operator_ftp_put]
    ftp_put = FTPFileTransmitOperator(
        task_id="test_ftp_put",
        ftp_conn_id="ftp_default",
        local_filepath="/tmp/filepath",
        remote_filepath="/remote_tmp/filepath",
        operation=FTPOperation.PUT,
        create_intermediate_dirs=True,
    )
    # [END howto_operator_ftp_put]

    # [START howto_operator_ftp_get]
    ftp_get = FTPFileTransmitOperator(
        task_id="test_ftp_get",
        ftp_conn_id="ftp_default",
        local_filepath="/tmp/filepath",
        remote_filepath="/remote_tmp/filepath",
        operation=FTPOperation.GET,
        create_intermediate_dirs=True,
    )
    # [END howto_operator_ftp_get]

    # [START howto_operator_ftps_put]
    ftps_put = FTPSFileTransmitOperator(
        task_id="test_ftps_put",
        ftp_conn_id="ftps_default",
        local_filepath="/tmp/filepath",
        remote_filepath="/remote_tmp/filepath",
        operation=FTPOperation.PUT,
        create_intermediate_dirs=True,
    )
    # [END howto_operator_ftps_put]

    # [START howto_operator_ftps_get]
    ftps_get = FTPSFileTransmitOperator(
        task_id="test_ftps_get",
        ftp_conn_id="ftps_default",
        local_filepath="/tmp/filepath",
        remote_filepath="/remote_tmp/filepath",
        operation=FTPOperation.GET,
        create_intermediate_dirs=True,
    )
    # [END howto_operator_ftps_get]

    ftp_put >> ftp_get
    ftps_put >> ftps_get

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
