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
"""Example DAG showing how to use the SandboxOperator with the ``local`` backend."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.sandbox.operators.sandbox import SandboxOperator

DAG_ID = "example_sandbox"

with DAG(
    DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "sandbox"],
) as dag:
    # [START howto_operator_sandbox]
    run_command = SandboxOperator(
        task_id="run_command",
        provider="local",
        env={"SECRET": "s3cr3t"},
        command='echo "injected=${SECRET:+yes}"; echo SYS_OK',
        poll_interval=1,
    )
    # [END howto_operator_sandbox]


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
