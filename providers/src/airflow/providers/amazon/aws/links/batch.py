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

from airflow.providers.amazon.aws.links.base_aws import BASE_AWS_CONSOLE_LINK, BaseAwsLink


class BatchJobDefinitionLink(BaseAwsLink):
    """Helper class for constructing AWS Batch Job Definition Link."""

    name = "Batch Job Definition"
    key = "batch_job_definition"
    format_str = (
        BASE_AWS_CONSOLE_LINK
        + "/batch/home?region={region_name}#job-definition/detail/{job_definition_arn}"
    )


class BatchJobDetailsLink(BaseAwsLink):
    """Helper class for constructing AWS Batch Job Details Link."""

    name = "Batch Job Details"
    key = "batch_job_details"
    format_str = (
        BASE_AWS_CONSOLE_LINK + "/batch/home?region={region_name}#jobs/detail/{job_id}"
    )


class BatchJobQueueLink(BaseAwsLink):
    """Helper class for constructing AWS Batch Job Queue Link."""

    name = "Batch Job Queue"
    key = "batch_job_queue"
    format_str = (
        BASE_AWS_CONSOLE_LINK
        + "/batch/home?region={region_name}#queues/detail/{job_queue_arn}"
    )
