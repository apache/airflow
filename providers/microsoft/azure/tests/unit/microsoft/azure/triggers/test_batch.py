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

import json

from airflow.providers.microsoft.azure.triggers.batch import AzureBatchJobTrigger


def test_azure_batch_job_trigger_serialize():
    trigger = AzureBatchJobTrigger(
        job_id="job-123",
        azure_batch_conn_id="azure_batch_conn",
        timeout=42,
        poll_interval=9,
    )

    class_path, payload = trigger.serialize()

    assert class_path == "airflow.providers.microsoft.azure.triggers.batch.AzureBatchJobTrigger"
    assert set(payload.keys()) == {"job_id", "azure_batch_conn_id", "timeout", "poll_interval"}
    assert payload["job_id"] == "job-123"
    assert payload["azure_batch_conn_id"] == "azure_batch_conn"
    assert payload["timeout"] == 42
    assert payload["poll_interval"] == 9
    json.dumps(payload)
    assert all(isinstance(value, (str, int, float, bool, type(None))) for value in payload.values())
