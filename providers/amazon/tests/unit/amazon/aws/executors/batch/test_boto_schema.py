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
from marshmallow import ValidationError

from airflow.providers.amazon.aws.executors.batch.boto_schema import (
    BatchDescribeJobsResponseSchema,
    BatchJobDetailSchema,
    BatchSubmitJobResponseSchema,
)
from airflow.providers.amazon.aws.executors.batch.utils import BatchJob


class TestBatchSubmitJobResponseSchema:
    def test_load_maps_camel_case_job_id_to_snake_case(self):
        loaded = BatchSubmitJobResponseSchema().load({"jobId": "job-1"})

        assert loaded == {"job_id": "job-1"}

    def test_load_ignores_fields_the_submit_job_api_adds(self):
        loaded = BatchSubmitJobResponseSchema().load(
            {"jobId": "job-1", "jobName": "airflow-task", "jobArn": "arn:aws:batch:job-1"}
        )

        assert loaded == {"job_id": "job-1"}

    def test_load_rejects_response_without_job_id(self):
        with pytest.raises(ValidationError) as excinfo:
            BatchSubmitJobResponseSchema().load({"jobName": "airflow-task"})

        assert "jobId" in excinfo.value.messages

    def test_load_rejects_snake_case_job_id(self):
        with pytest.raises(ValidationError) as excinfo:
            BatchSubmitJobResponseSchema().load({"job_id": "job-1"})

        assert "jobId" in excinfo.value.messages


class TestBatchJobDetailSchema:
    def test_load_returns_batch_job_instead_of_dict(self):
        job = BatchJobDetailSchema().load({"jobId": "job-1", "status": "RUNNING", "statusReason": "started"})

        assert isinstance(job, BatchJob)
        assert job.job_id == "job-1"
        assert job.status == "RUNNING"
        assert job.status_reason == "started"

    def test_load_without_status_reason_leaves_it_unset(self):
        job = BatchJobDetailSchema().load({"jobId": "job-1", "status": "SUCCEEDED"})

        assert job.status_reason is None

    def test_load_ignores_fields_the_describe_jobs_api_adds(self):
        job = BatchJobDetailSchema().load(
            {"jobId": "job-1", "status": "FAILED", "jobQueue": "queue-1", "startedAt": 1700000000}
        )

        assert isinstance(job, BatchJob)
        assert job.job_id == "job-1"

    @pytest.mark.parametrize(
        ("payload", "missing_key"),
        [
            pytest.param({"status": "RUNNING"}, "jobId", id="without-job-id"),
            pytest.param({"jobId": "job-1"}, "status", id="without-status"),
        ],
    )
    def test_load_rejects_detail_missing_a_required_field(self, payload, missing_key):
        with pytest.raises(ValidationError) as excinfo:
            BatchJobDetailSchema().load(payload)

        assert missing_key in excinfo.value.messages


class TestBatchDescribeJobsResponseSchema:
    def test_load_returns_a_batch_job_per_entry(self):
        loaded = BatchDescribeJobsResponseSchema().load(
            {
                "jobs": [
                    {"jobId": "job-1", "status": "RUNNING"},
                    {"jobId": "job-2", "status": "FAILED", "statusReason": "exit code 1"},
                ]
            }
        )

        assert [(job.job_id, job.status, job.status_reason) for job in loaded["jobs"]] == [
            ("job-1", "RUNNING", None),
            ("job-2", "FAILED", "exit code 1"),
        ]
        assert all(isinstance(job, BatchJob) for job in loaded["jobs"])

    def test_load_accepts_an_empty_job_list(self):
        loaded = BatchDescribeJobsResponseSchema().load({"jobs": []})

        assert loaded == {"jobs": []}

    def test_load_rejects_response_without_jobs(self):
        with pytest.raises(ValidationError) as excinfo:
            BatchDescribeJobsResponseSchema().load({})

        assert "jobs" in excinfo.value.messages

    def test_load_propagates_validation_error_from_a_nested_job(self):
        with pytest.raises(ValidationError) as excinfo:
            BatchDescribeJobsResponseSchema().load({"jobs": [{"jobId": "job-1"}]})

        assert "status" in excinfo.value.messages["jobs"][0]
