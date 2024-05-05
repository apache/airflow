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

import jmespath
import pytest

from tests.charts.helm_template_generator import render_chart


class TestPlaceHolder:

    def test_priority_class_name(self):
        docs = render_chart(
            values={
                "flower": {"enabled": True, "priorityClassName": "low-priority-flower"},
                "pgbouncer": {"enabled": True, "priorityClassName": "low-priority-pgbouncer"},
                "scheduler": {"priorityClassName": "low-priority-scheduler"},
                "statsd": {"priorityClassName": "low-priority-statsd"},
                "triggerer": {"priorityClassName": "low-priority-triggerer"},
                "dagProcessor": {"priorityClassName": "low-priority-dag-processor"},
                "webserver": {"priorityClassName": "low-priority-webserver"},
                "workers": {"priorityClassName": "low-priority-worker"},
            },
            show_only=[
                "templates/flower/flower-deployment.yaml",
                "templates/pgbouncer/pgbouncer-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
            ],
        )
        assert 7 == len(docs)
        for doc in docs:
            component = doc["metadata"]["labels"]["component"]
            priority = doc["spec"]["template"]["spec"]["priorityClassName"]

            assert priority == f"low-priority-{component}"
