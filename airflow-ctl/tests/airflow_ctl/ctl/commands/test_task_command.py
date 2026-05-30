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

import datetime
from uuid import uuid4

from airflowctl.api.client import ClientKind
from airflowctl.api.datamodels.generated import TaskInstanceResponse, TaskInstanceState
from airflowctl.ctl import cli_parser


class TestTaskCommands:
    parser = cli_parser.get_parser()

    def test_task_state(self, api_client_maker, capsys):
        response = TaskInstanceResponse(
            id=uuid4(),
            task_id="runme_0",
            dag_id="example_bash_operator",
            dag_run_id="manual__2025-01-01T00:00:00+00:00",
            map_index=-1,
            logical_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
            run_after=datetime.datetime(2025, 1, 1, 0, 0, 0),
            start_date=None,
            end_date=None,
            duration=None,
            state=TaskInstanceState.SUCCESS,
            try_number=1,
            max_tries=1,
            task_display_name="runme_0",
            dag_display_name="example_bash_operator",
            hostname=None,
            unixname=None,
            pool="default_pool",
            pool_slots=1,
            queue=None,
            priority_weight=None,
            operator=None,
            operator_name=None,
            queued_when=None,
            scheduled_when=None,
            pid=None,
            executor=None,
            executor_config="{}",
            note=None,
            rendered_map_index=None,
            rendered_fields=None,
            trigger=None,
            triggerer_job=None,
            dag_version=None,
        )
        api_client = api_client_maker(
            path="/api/v2/dags/example_bash_operator/dagRuns/manual__2025-01-01T00:00:00+00:00/taskInstances/runme_0",
            response_json=response.model_dump(mode="json"),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )

        args = self.parser.parse_args(
            [
                "tasks",
                "state",
                "example_bash_operator",
                "manual__2025-01-01T00:00:00+00:00",
                "runme_0",
            ]
        )

        args.func(args, api_client=api_client)

        captured = capsys.readouterr()
        assert '"state": "success"' in captured.out
