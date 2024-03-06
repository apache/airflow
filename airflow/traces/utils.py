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

import logging
from airflow.utils.hashlib_wrapper import md5
from airflow.traces import (
    TRACEPARENT,
)

log = logging.getLogger(__name__)


def gen_trace_id(dag_run)->str:
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    start_dt = dag_run.start_date
    hash_seed = f"{dag_id}_{run_id}_{start_dt.timestamp()}"
    hash_hex =  md5(hash_seed.encode("utf-8")).hexdigest()
    return hash_hex


def gen_span_id_from_ti_key(ti_key)->str:
    """generate span id from TI key"""
    dag_id = ti_key.dag_id
    run_id = ti_key.run_id
    task_id = ti_key.task_id
    try_num = ti_key.try_number   # key always has next number, not current
    hash_seed = f"{dag_id}_{run_id}_{task_id}_{try_num}"
    hash_hex =  md5(hash_seed.encode("utf-8")).hexdigest()[16:]
    log.info(f"[gen_span_id_from_ti_key] dag_id: {dag_id} run_id: {run_id} task_id: {task_id} try_num: {try_num} => {hash_hex}")
    return hash_hex


def gen_dag_span_id(dag_run):
    """generate dag's root span id using dag_run"""
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    start_dt = dag_run.start_date
    hash_seed = f"{dag_id}_{run_id}_{start_dt.timestamp()}"
    hash_hex = md5(hash_seed.encode("utf-8")).hexdigest()[16:]
    return hash_hex


def gen_span_id(ti):
    """generate span id from the task instance"""
    dag_run = ti.dag_run
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    task_id = ti.task_id
    """in terms of ti when this is called, the try_number is already set to next, hence the subtraction"""
    try_num = ti.try_number-1
    hash_seed = f"{dag_id}_{run_id}_{task_id}_{try_num}"
    hash_hex = md5(hash_seed.encode("utf-8")).hexdigest()[16:]
    log.info(f"[gen_span_id] dag_id: {dag_id} run_id: {run_id} task_id: {task_id} try_num: {try_num} => {hash_hex}")
    return hash_hex


def parse_traceparent(traceparent_str:str) -> dict:
    """parse traceparent string: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01 """
    if traceparent_str is None:
        return {}
    tokens = traceparent_str.split("-")
    if len(tokens) != 4:
        raise ValueError("The traceparent string does not have the correct format.")
    return {
        'version': tokens[0],
        'trace_id': tokens[1],
        'parent_id': tokens[2],
        'flags': tokens[3]
    }


def parse_tracestate(tracestate_str:str) -> dict:
    """parse tracestate string: rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"""
    if tracestate_str is None:
        return {}
    tokens = tracestate_str.split(",")
    result = {}
    for pair in tokens:
        key, value = pair.split("=")
        result[key.strip()] = value.strip()
    return result


def is_valid_trace_id(trace_id:str) -> bool:
    if trace_id is not None and len(trace_id) == 32 and trace_id != "0x00000000000000000000000000000000":
        return True
    else:
        return False


def is_valid_span_id(span_id:str) -> bool:
    if span_id is not None and len(span_id) == 16 and span_id != "0x0000000000000000":
        return True
    else:
        return False