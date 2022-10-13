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

import copy
import typing

from airflow.listeners import hookimpl

if typing.TYPE_CHECKING:
    from airflow.models.dagrun import DagRun


running, success, failure = [], [], []


@hookimpl
def on_dag_run_running(dag_run: DagRun, msg: str):
    running.append(copy.deepcopy(dag_run))


@hookimpl
def on_dag_run_success(dag_run: DagRun, msg: str):
    success.append(copy.deepcopy(dag_run))


@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str):
    failure.append(dag_run)


def clear():
    global running, success, failure
    running, success, failure = [], [], []
