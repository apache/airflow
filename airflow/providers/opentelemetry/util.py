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
from airflow.utils.hashlib_wrapper import md5
from airflow.utils.state import TaskInstanceState

log = logging.getLogger(__name__)

def gen_trace_id(dag_run) -> str:
    """Generate trace id from dag_run."""
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    start_dt = dag_run.start_date
    hash_seed = f"{dag_id}_{run_id}_{start_dt.timestamp()}"
    hash_hex = md5(hash_seed.encode("utf-8")).hexdigest()
    return hash_hex


def gen_dag_span_id(dag_run) -> str:
    """Generate dag's root span id using dag_run."""
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    start_dt = dag_run.start_date
    hash_seed = f"{dag_id}_{run_id}_{start_dt.timestamp()}"
    hash_hex = md5(hash_seed.encode("utf-8")).hexdigest()[16:]
    return hash_hex


def gen_span_id(ti) -> str:
    """Generate span id from the task instance."""
    dag_run = ti.dag_run
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    task_id = ti.task_id
    if ti.state == TaskInstanceState.SUCCESS or ti.state == TaskInstanceState.FAILED:
        try_num = ti.try_number - 1
    else:
        try_num = ti.try_number

    hash_seed = f"{dag_id}_{run_id}_{task_id}_{try_num}"
    hash_hex = md5(hash_seed.encode("utf-8")).hexdigest()[16:]
    return hash_hex


def datetime_to_nano(datetime) -> int:
    """Convert datetime to nanoseconds."""
    return int(datetime.timestamp() * 1000000000)
