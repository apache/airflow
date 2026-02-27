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
import os
import time
from datetime import datetime

from airflow import DAG
from airflow.sdk import chain, task
from airflow.sdk.opentelemetry import trace

logger = logging.getLogger(__name__)

tracer = trace.get_tracer(__name__)

args = {
    "owner": "airflow",
    "start_date": datetime(2024, 9, 2),
    "retries": 0,
}


@task
def task1(ti):
    logger.info("Starting Task_1.")

    dag_folder = os.path.dirname(os.path.abspath(__file__))
    control_file = os.path.join(dag_folder, "dag_control.txt")

    # Create the file and write 'pause' to it.
    with open(control_file, "w") as file:
        file.write("pause")

    # Pause execution until the word 'pause' is replaced on the file.
    while True:
        # If there is an exception, then writing to the file failed. Let it exit.
        file_contents = None
        with open(control_file) as file:
            file_contents = file.read()

        if "pause" in file_contents:
            logger.info("Task has been paused.")
            time.sleep(1)
            continue
        logger.info("Resuming task execution.")
        # Break the loop and finish with the task execution.
        break

    with tracer.start_as_current_span(
        span_name="task1_sub_span1",
        component="dag",
    ) as s1:
        s1.set_attribute("attr1", "val1")
        logger.info("From task sub_span1.")

        with tracer.start_as_current_span("task1_sub_span2") as s2:
            s2.set_attribute("attr2", "val2")
            logger.info("From task sub_span2.")

    with tracer.start_child_span(
        span_name="task1_sub_span4",
        component="dag",
    ) as s4:
        s4.set_attribute("attr4", "val4")
        logger.info("From task sub_span4.")

    # Cleanup the control file.
    if os.path.exists(control_file):
        os.remove(control_file)
        print("Control file has been cleaned up.")

    logger.info("Task_1 finished.")


@task
def task2():
    logger.info("Starting Task_2.")
    for i in range(3):
        logger.info("Task_2, iteration '%d'.", i)
    logger.info("Task_2 finished.")


with DAG(
    "otel_test_dag_with_pause_in_task",
    default_args=args,
    schedule=None,
    catchup=False,
) as dag:
    chain(task1(), task2())  # type: ignore
