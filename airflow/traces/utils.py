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
    """generate trace id using dag_run as input
       if traceparent exists inside dag_run's conf,
       use it instead"""
    conf = dag_run.conf
    if conf is not None and TRACEPARENT in conf:
        traceparent = conf.get(TRACEPARENT)
        trace_ctx = parse_traceparent(traceparent)
        return trace_ctx['trace_id']
    else:
        dag_hash = dag_run.dag_hash
        run_type = dag_run.run_type
        start_date = dag_run.start_date.isoformat()
        hash_seed = f"{dag_hash}_{run_type}_{start_date}"
        hash_hex =  md5(hash_seed.encode("utf-8")).hexdigest()
        return hash_hex


def gen_dag_span_id(dag_run):
    """generate dag's root span id using dag_run"""
    dag_hash = dag_run.dag_hash
    run_type = dag_run.run_type
    start_date = dag_run.start_date.isoformat()
    hash_seed = f"{dag_hash}_{run_type}_{start_date}"
    hash_hex = md5(hash_seed.encode("utf-8")).hexdigest()[16:]
    return hash_hex


def gen_span_id(ti):
    """generate span id from the task instance"""
    dag_run = ti.dag_run
    dag_hash = dag_run.dag_hash
    run_type = dag_run.run_type
    start_date = dag_run.start_date.isoformat()
    task_id = ti.task_id
    hash_seed = f"{dag_hash}_{run_type}_{start_date}_{task_id}"
    hash_hex = md5(hash_seed.encode("utf-8")).hexdigest()[16:]
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