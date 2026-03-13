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

from opentelemetry import trace

from airflow import DAG
from airflow.sdk import task

logger = logging.getLogger("airflow.otel_test_dag")

tracer = trace.get_tracer(__name__)

args = {
    "owner": "airflow",
    "start_date": datetime(2024, 9, 1),
    "retries": 0,
}


@task
def task1():
    logger.info("starting task1")

    with tracer.start_as_current_span("sub_span1") as s1:
        s1.set_attribute("attr1", "val1")

    logger.info("task1 finished.")


with DAG(
    "otel_test_dag",
    default_args=args,
    schedule=None,
    catchup=False,
) as dag:
    task1()
