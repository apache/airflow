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
Stub Dag of the Java test bundle whose stub tasks do not match their task handlers.

Each task is a different problem the Dag processor reports as one import error of this file: no artifact
registers ``not_registered``, and ``takes_two_numbers`` is called with one argument more than its handler
takes.
"""

from __future__ import annotations

from airflow.sdk import dag, task

_QUEUE = "java-test"


@task.stub(queue=_QUEUE)
def not_registered(): ...


@task.stub(queue=_QUEUE)
def takes_two_numbers(first: int, second: int, third: int): ...


@dag(dag_id="java_task_handler_failures")
def java_task_handler_failures():
    not_registered()
    takes_two_numbers(1, 2, 3)


java_task_handler_failures()
