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

from marshmallow import EXCLUDE, Schema, fields, post_load

from airflow.providers.amazon.aws.executors.batch.utils import BatchJob


class BatchSubmitJobResponseSchema(Schema):
    """API Response for SubmitJob."""

    # The unique identifier for the job.
    job_id = fields.String(data_key="jobId", required=True)

    class Meta:
        """Options object for a Schema. See Schema.Meta for more details and valid values."""

        unknown = EXCLUDE


class BatchJobDetailSchema(Schema):
    """API Response for Describe Jobs."""

    # The unique identifier for the job.
    job_id = fields.String(data_key="jobId", required=True)
    # The current status for the job:
    # 'SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING', 'SUCCEEDED', 'FAILED'
    status = fields.String(required=True)
    # A short, human-readable string to provide additional details about the current status of the job.
    status_reason = fields.String(data_key="statusReason")

    @post_load
    def make_job(self, data, **kwargs):
        """Overwrite marshmallow load() to return an instance of BatchJob instead of a dictionary."""
        return BatchJob(**data)

    class Meta:
        """Options object for a Schema. See Schema.Meta for more details and valid values."""

        unknown = EXCLUDE


class BatchDescribeJobsResponseSchema(Schema):
    """API Response for Describe Jobs."""

    # The list of jobs
    jobs = fields.List(fields.Nested(BatchJobDetailSchema), required=True)

    class Meta:
        """Options object for a Schema. See Schema.Meta for more details and valid values."""

        unknown = EXCLUDE
