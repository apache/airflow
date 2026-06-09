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
Sample DAG that intentionally fails for triage testing.

Trigger manually from the Airflow UI, then open the Triage Panel at
``/triage-panel/`` to see how the plugin classifies each failure.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, task


@task
def succeed():
    """Run a healthy task without error."""
    print("All systems go.")


@task
def fail_import_error():
    """Simulate a missing-dependency failure (CODE category)."""
    raise ImportError("No module named 'nonexistent_lib'")


@task
def fail_timeout():
    """Simulate a transient network timeout (TRANSIENT category)."""
    raise TimeoutError("Task failed after 30s: connection reset by peer, timeout exceeded")


@task
def fail_oom():
    """Simulate an out-of-memory failure (RESOURCE category)."""
    raise MemoryError("Container OOMKilled: memory limit of 512Mi exceeded")


@task
def fail_key_error():
    """Simulate a code bug (CODE category)."""
    data: dict = {}
    _ = data["missing_key"]


with DAG(
    dag_id="sample_failing_dag",
    description="Intentionally failing DAG for triage plugin testing",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["triage-test", "sample"],
) as dag:
    succeed() >> [fail_import_error(), fail_timeout(), fail_oom(), fail_key_error()]
