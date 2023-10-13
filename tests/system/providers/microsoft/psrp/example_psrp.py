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
This is an example dag for using the PsrpOperator.
"""
from __future__ import annotations

# --------------------------------------------------------------------------------
# Load The Dependencies
# --------------------------------------------------------------------------------
from datetime import datetime, timedelta

from airflow import DAG

# --------------------------------------------------------------------------------
# The purpose of this is to give you a sample of a real world example DAG!
# --------------------------------------------------------------------------------


try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

from airflow.providers.microsoft.psrp.operators.psrp import PsrpOperator

DAG_ID = "PSRP_EXAMPLE_DAG"

default_args = {"psrp_conn_id": "the_conn_id"}

with DAG(
    dag_id=DAG_ID,
    schedule="0 0 * * *",
    start_date=datetime(2203, 10, 1),
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=60),
    tags=["example-psrp"],
    catchup=False,
) as dag:
    run_this_last = EmptyOperator(task_id="run_this_last")

    # [START run_operator]
    t1 = PsrpOperator(
        task_id="USE_CMDLET",
        cmdlet="Out-Default",
        parameters={"InputObject": "Hello, world."},
    )

    t2 = PsrpOperator(task_id="EXEC_POWERSHELL", powershell="sleep 60")

    # [END run_operator]

    [t1, t2] >> run_this_last

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
