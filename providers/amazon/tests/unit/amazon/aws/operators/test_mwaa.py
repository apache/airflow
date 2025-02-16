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

from unittest import mock

from airflow.providers.amazon.aws.operators.mwaa import MwaaTriggerDagRunOperator
from unit.amazon.aws.utils.test_template_fields import validate_template_fields

OP_KWARGS = {
    "task_id": "test_task",
    "env_name": "test_env",
    "trigger_dag_id": "test_dag_id",
    "trigger_run_id": "test_run_id",
    "logical_date": "2025-01-01T00:00:01Z",
    "data_interval_start": "2025-01-02T00:00:01Z",
    "data_interval_end": "2025-01-03T00:00:01Z",
    "conf": {"key": "value"},
    "note": "test note",
}
HOOK_RETURN_VALUE = {
    "ResponseMetadata": {},
    "RestApiStatusCode": 200,
    "RestApiResponse": {
        "dag_run_id": "manual__2025-02-08T00:33:09.457198+00:00",
        "other_key": "value",
    },
}


class TestMwaaTriggerDagRunOperator:
    def test_init(self):
        op = MwaaTriggerDagRunOperator(**OP_KWARGS)
        assert op.env_name == OP_KWARGS["env_name"]
        assert op.trigger_dag_id == OP_KWARGS["trigger_dag_id"]
        assert op.trigger_run_id == OP_KWARGS["trigger_run_id"]
        assert op.logical_date == OP_KWARGS["logical_date"]
        assert op.data_interval_start == OP_KWARGS["data_interval_start"]
        assert op.data_interval_end == OP_KWARGS["data_interval_end"]
        assert op.conf == OP_KWARGS["conf"]
        assert op.note == OP_KWARGS["note"]

    @mock.patch.object(MwaaTriggerDagRunOperator, "hook")
    def test_execute(self, mock_hook):
        mock_hook.invoke_rest_api.return_value = HOOK_RETURN_VALUE
        op = MwaaTriggerDagRunOperator(**OP_KWARGS)
        op_ret_val = op.execute({})

        mock_hook.invoke_rest_api.assert_called_once_with(
            env_name=OP_KWARGS["env_name"],
            path=f"/dags/{OP_KWARGS['trigger_dag_id']}/dagRuns",
            method="POST",
            body={
                "dag_run_id": OP_KWARGS["trigger_run_id"],
                "logical_date": OP_KWARGS["logical_date"],
                "data_interval_start": OP_KWARGS["data_interval_start"],
                "data_interval_end": OP_KWARGS["data_interval_end"],
                "conf": OP_KWARGS["conf"],
                "note": OP_KWARGS["note"],
            },
        )
        assert op_ret_val == HOOK_RETURN_VALUE

    def test_template_fields(self):
        operator = MwaaTriggerDagRunOperator(**OP_KWARGS)
        validate_template_fields(operator)
