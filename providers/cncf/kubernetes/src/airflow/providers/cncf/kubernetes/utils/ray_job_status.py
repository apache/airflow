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
"""Status constants and helpers for KubeRay RayJob CRDs."""

from __future__ import annotations


class RayJobStatus:
    """Ray Job status constants from the KubeRay RayJob CRD."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class RayJobDeploymentStatus:
    """Ray Job deployment status constants from the KubeRay RayJob CRD."""

    INITIALIZING = "Initializing"
    RUNNING = "Running"
    COMPLETE = "Complete"
    FAILED = "Failed"
    VALIDATION_FAILED = "ValidationFailed"
    SUSPENDING = "Suspending"
    SUSPENDED = "Suspended"


TERMINAL_JOB_STATUSES = frozenset({RayJobStatus.SUCCEEDED, RayJobStatus.FAILED, RayJobStatus.STOPPED})
TERMINAL_DEPLOYMENT_STATUSES = frozenset(
    {RayJobDeploymentStatus.COMPLETE, RayJobDeploymentStatus.FAILED, RayJobDeploymentStatus.VALIDATION_FAILED}
)


def is_ray_job_terminal(job_status: str, deployment_status: str) -> bool:
    """Check if a RayJob is in a terminal state."""
    return job_status in TERMINAL_JOB_STATUSES or deployment_status in TERMINAL_DEPLOYMENT_STATUSES


def is_ray_job_successful(job_status: str, deployment_status: str) -> bool:
    """Check if a RayJob completed successfully."""
    return job_status == RayJobStatus.SUCCEEDED or (
        deployment_status == RayJobDeploymentStatus.COMPLETE and job_status == RayJobStatus.SUCCEEDED
    )
