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
A merge gate whose wait has no timeout, which the API server refuses without a default timeout.

Without ``timeout``, WaitForWebhookOperator takes ``[webhooks] default_wait_timeout``. demo.sh leaves that
option unset, so the register-wait route answers 422 with the reason ``timeout_required``, and the task
fails without parking.
"""

from __future__ import annotations

import os

from airflow.providers.standard.operators.webhooks import WaitForWebhookOperator
from airflow.sdk import Param, dag

REPOSITORY = os.environ.get("GITHUB_DEMO_REPO") or "my-org/configs"


@dag(schedule=None, catchup=False, params={"pr_number": Param(type="integer")})
def merge_gate_no_timeout():
    WaitForWebhookOperator(
        task_id="wait_for_merge",
        webhook="github",
        events=["pull_request"],
        match={"repository": REPOSITORY, "number": "{{ params.pr_number }}", "action": "closed"},
    )


merge_gate_no_timeout()
