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

from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.links.base_aws import BASE_AWS_CONSOLE_LINK, BaseAwsLink
from airflow.utils.helpers import exactly_one

if TYPE_CHECKING:
    import boto3


class EmrClusterLink(BaseAwsLink):
    """Helper class for constructing AWS EMR Cluster Link."""

    name = "EMR Cluster"
    key = "emr_cluster"
    format_str = BASE_AWS_CONSOLE_LINK + "/emr/home?region={region_name}#/clusterDetails/{job_flow_id}"


class EmrLogsLink(BaseAwsLink):
    """Helper class for constructing AWS EMR Logs Link."""

    name = "EMR Cluster Logs"
    key = "emr_logs"
    format_str = BASE_AWS_CONSOLE_LINK + "/s3/buckets/{log_uri}?region={region_name}&prefix={job_flow_id}/"

    def format_link(self, **kwargs) -> str:
        if not kwargs.get("log_uri"):
            return ""
        return super().format_link(**kwargs)


def get_log_uri(
    *, cluster: dict[str, Any] | None = None, emr_client: boto3.client = None, job_flow_id: str | None = None
) -> str | None:
    """
    Retrieve the S3 URI to the EMR Job logs.

    Requires either the output of a describe_cluster call or both an EMR Client and a job_flow_id..
    """
    if not exactly_one(bool(cluster), emr_client and job_flow_id):
        raise AirflowException(
            "Requires either the output of a describe_cluster call or both an EMR Client and a job_flow_id."
        )

    cluster_info = (cluster or emr_client.describe_cluster(ClusterId=job_flow_id))["Cluster"]
    if "LogUri" not in cluster_info:
        return None
    log_uri = S3Hook.parse_s3_url(cluster_info["LogUri"])
    return "/".join(log_uri)
