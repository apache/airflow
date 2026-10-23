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
from chart_utils.helm_template_generator import render_chart


class TestGitSync:
    @pytest.mark.parametrize(
        ("template", "executor"),
        [
            pytest.param("scheduler/scheduler-deployment", "LocalExecutor", id="scheduler"),
            pytest.param("dag-processor/dag-processor-deployment", "CeleryExecutor", id="dag-processor"),
            pytest.param("workers/worker-deployment", "CeleryExecutor", id="worker"),
            pytest.param("triggerer/triggerer-deployment", "CeleryExecutor", id="triggerer"),
        ],
    )
    @pytest.mark.parametrize(
        ("max_failures", "expected"),
        [
            pytest.param(None, "5", id="default"),
            pytest.param(0, "0", id="no-retries"),
            pytest.param(-1, "-1", id="unlimited-retries"),
            pytest.param(70, "70", id="custom-retries"),
        ],
    )
    def test_max_failures(self, template, executor, max_failures, expected):
        git_sync_values = {"enabled": True}
        if max_failures is not None:
            git_sync_values["maxFailures"] = max_failures

        docs = render_chart(
            values={"executor": executor, "dags": {"gitSync": git_sync_values}},
            show_only=[f"templates/{template}.yaml"],
        )

        for containers, container_name in [("containers", "git-sync"), ("initContainers", "git-sync-init")]:
            env = jmespath.search(
                f"spec.template.spec.{containers}[?name=='{container_name}'] | [0].env", docs[0]
            )
            assert {"name": "GIT_SYNC_MAX_SYNC_FAILURES", "value": expected} in env
            assert {"name": "GITSYNC_MAX_FAILURES", "value": expected} in env
