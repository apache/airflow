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

from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import (
    ADOPTED,
    ALL_NAMESPACES,
    POD_EXECUTOR_DONE_KEY,
    POD_REVOKED_KEY,
    FailureDetails,
    KubernetesJob,
)
from airflow.providers.common.compat.sdk import TaskInstanceKey


def test_pod_annotation_and_label_keys_are_stable():
    """These values are persisted on live pods; renaming them breaks adoption of running tasks."""
    assert ADOPTED == "adopted"
    assert ALL_NAMESPACES == "ALL_NAMESPACES"
    assert POD_EXECUTOR_DONE_KEY == "airflow_executor_done"
    assert POD_REVOKED_KEY == "airflow_pod_revoked"


def test_kubernetes_job_defaults_to_no_image_override():
    job = KubernetesJob(
        key=TaskInstanceKey("dag", "task", "run", 1, -1),
        command=["airflow", "tasks", "run"],
        kube_executor_config=None,
        pod_template_file=None,
    )

    assert job.kube_image is None


def test_failure_details_fields_are_all_optional():
    assert FailureDetails.__total__ is False
    assert FailureDetails(exit_code=1) == {"exit_code": 1}
