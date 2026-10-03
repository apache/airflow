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
Stub Dag of the Go test bundle whose task the Dag processor never binds.

Its queue is routed to a Lang-SDK coordinator by the scheduler and the worker but not by the Dag processor,
so the file parses without an error, and the task reaches the worker without an artifact. ``handlers_a``
does register a handler for it, which shows that nothing searches for one.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.sdk import dag, task


@task.stub(queue="golang-unbound", retries=1, retry_delay=timedelta(seconds=1))
def unbound(): ...


@dag(dag_id="go_unbound_stub")
def go_unbound_stub():
    unbound()


go_unbound_stub()
