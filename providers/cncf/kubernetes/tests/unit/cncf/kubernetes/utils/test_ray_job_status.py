#
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

import pytest

from airflow.providers.cncf.kubernetes.utils.ray_job_status import (
    RayJobDeploymentStatus,
    RayJobStatus,
    is_ray_job_successful,
    is_ray_job_terminal,
)


class TestRayJobStatus:
    @pytest.mark.parametrize(
        ("job_status", "dep_status", "expected"),
        [
            pytest.param(RayJobStatus.SUCCEEDED, RayJobDeploymentStatus.COMPLETE, True, id="succeeded"),
            pytest.param(RayJobStatus.FAILED, RayJobDeploymentStatus.FAILED, True, id="failed"),
            pytest.param(RayJobStatus.STOPPED, RayJobDeploymentStatus.COMPLETE, True, id="stopped"),
            pytest.param(RayJobStatus.RUNNING, RayJobDeploymentStatus.RUNNING, False, id="running"),
            pytest.param(RayJobStatus.PENDING, RayJobDeploymentStatus.INITIALIZING, False, id="pending"),
            pytest.param("", "", False, id="empty"),
            pytest.param("", RayJobDeploymentStatus.VALIDATION_FAILED, True, id="validation_failed"),
        ],
    )
    def test_is_ray_job_terminal(self, job_status, dep_status, expected):
        assert is_ray_job_terminal(job_status, dep_status) is expected

    @pytest.mark.parametrize(
        ("job_status", "dep_status", "expected"),
        [
            pytest.param(RayJobStatus.SUCCEEDED, RayJobDeploymentStatus.COMPLETE, True, id="succeeded"),
            pytest.param(RayJobStatus.SUCCEEDED, "", True, id="succeeded_no_dep"),
            pytest.param(RayJobStatus.FAILED, RayJobDeploymentStatus.FAILED, False, id="failed"),
            pytest.param(RayJobStatus.RUNNING, RayJobDeploymentStatus.RUNNING, False, id="running"),
            pytest.param(RayJobStatus.STOPPED, RayJobDeploymentStatus.COMPLETE, False, id="stopped"),
        ],
    )
    def test_is_ray_job_successful(self, job_status, dep_status, expected):
        assert is_ray_job_successful(job_status, dep_status) is expected
