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

from airflow.providers.remote.models.remote_logs import RemoteLogs, RemoteLogsModel
from airflow.utils import timezone


def test_serializing_pydantic_remote_logs():
    rlm = RemoteLogsModel(
        dag_id="test_dag",
        task_id="test_task",
        run_id="test_run",
        map_index=-1,
        try_number=1,
        log_chunk_time=timezone.utcnow(),
        log_chunk_data="some logs captured",
    )

    pydantic_logs = RemoteLogs.model_validate(rlm)

    json_string = pydantic_logs.model_dump_json()
    print(json_string)

    deserialized_model = RemoteLogs.model_validate_json(json_string)
    assert deserialized_model.dag_id == rlm.dag_id
    assert deserialized_model.try_number == rlm.try_number
    assert deserialized_model.log_chunk_time == rlm.log_chunk_time
    assert deserialized_model.log_chunk_data == rlm.log_chunk_data
