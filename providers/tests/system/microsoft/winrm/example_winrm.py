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
This is an example dag for using the WinRMOperator.
"""

from __future__ import annotations

# --------------------------------------------------------------------------------
# Load The Dependencies
# --------------------------------------------------------------------------------
import os
from datetime import datetime, timedelta

from airflow import DAG

# --------------------------------------------------------------------------------
# Caveat: This Dag will not run because of missing scripts.
# The purpose of this is to give you a sample of a real world example DAG!
# --------------------------------------------------------------------------------
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.winrm.hooks.winrm import WinRMHook
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "POC_winrm_parallel"

with DAG(
    dag_id=DAG_ID,
    schedule="0 0 * * *",
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["example"],
    catchup=False,
) as dag:
    run_this_last = EmptyOperator(task_id="run_this_last")

    # [START create_hook]
    winRMHook = WinRMHook(ssh_conn_id="ssh_POC1")
    # [END create_hook]

    # [START run_operator]
    t1 = WinRMOperator(task_id="wintask1", command="ls -altr", winrm_hook=winRMHook)

    t2 = WinRMOperator(task_id="wintask2", command="sleep 60", winrm_hook=winRMHook)

    t3 = WinRMOperator(task_id="wintask3", command="echo 'luke test' ", winrm_hook=winRMHook)
    # [END run_operator]

    [t1, t2, t3] >> run_this_last

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
