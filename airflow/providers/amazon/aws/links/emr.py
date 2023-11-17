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
from urllib.parse import quote_plus, urlparse

from airflow.exceptions import AirflowException
from airflow.models import XCom
from airflow.providers.amazon.aws.hooks.emr import EmrServerlessHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.links.base_aws import BASE_AWS_CONSOLE_LINK, BaseAwsLink
from airflow.utils.helpers import exactly_one

if TYPE_CHECKING:
    import boto3

    from airflow.models import BaseOperator
    from airflow.models.taskinstancekey import TaskInstanceKey


class EmrClusterLink(BaseAwsLink):
    """Helper class for constructing Amazon EMR Cluster Link."""

    name = "EMR Cluster"
    key = "emr_cluster"
    format_str = BASE_AWS_CONSOLE_LINK + "/emr/home?region={region_name}#/clusterDetails/{job_flow_id}"


class EmrLogsLink(BaseAwsLink):
    """Helper class for constructing Amazon EMR Logs Link."""

    name = "EMR Cluster Logs"
    key = "emr_logs"
    format_str = BASE_AWS_CONSOLE_LINK + "/s3/buckets/{log_uri}?region={region_name}&prefix={job_flow_id}/"

    def format_link(self, **kwargs) -> str:
        if not kwargs["log_uri"]:
            return ""
        return super().format_link(**kwargs)


def get_serverless_log_uri(*, s3_log_uri: str, application_id: str, job_run_id: str) -> str:
    """
    Retrieves the S3 URI to EMR Serverless Job logs.

    Any EMR Serverless job may have a different S3 logging location (or none), which is an S3 URI.
    The logging location is then {s3_uri}/applications/{application_id}/jobs/{job_run_id}.
    """
    return f"{s3_log_uri}/applications/{application_id}/jobs/{job_run_id}"


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


class EmrServerlessLogsLink(BaseAwsLink):
    """Helper class for constructing Amazon EMR Serverless link to Spark stdout logs."""

    name = "Spark Driver stdout"
    key = "emr_serverless_logs"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        """
        Pre-signed URL to the Spark stdout log.

        :param operator: airflow operator
        :param ti_key: TaskInstance ID to return link for
        :return: Pre-signed URL to Spark stdout log. Empty string if no Spark stdout log is available.
        """
        conf = XCom.get_value(key=self.key, ti_key=ti_key)
        if not conf:
            return ""
        hook = EmrServerlessHook(aws_conn_id=conf.get("conn_id"))
        resp = hook.conn.get_dashboard_for_job_run(
            applicationId=conf.get("application_id"), jobRunId=conf.get("job_run_id")
        )
        o = urlparse(resp["url"])
        return o._replace(path="/logs/SPARK_DRIVER/stdout.gz").geturl()


class EmrServerlessDashboardLink(BaseAwsLink):
    """Helper class for constructing Amazon EMR Serverless Dashboard Link."""

    name = "EMR Serverless Dashboard"
    key = "emr_serverless_dashboard"

    def get_link(
        self,
        operator: BaseOperator,
        *,
        ti_key: TaskInstanceKey,
    ) -> str:
        """
        Pre-signed URL to the application UI for the EMR Serverless job.

        :param operator: airflow operator
        :param ti_key: TaskInstance ID to return link for
        :return: Pre-signed URL to application UI.
        """
        conf = XCom.get_value(key=self.key, ti_key=ti_key)
        if not conf:
            return ""
        hook = EmrServerlessHook(aws_conn_id=conf.get("conn_id"))
        # Dashboard cannot be served when job is pending/scheduled
        resp = hook.conn.get_dashboard_for_job_run(
            applicationId=conf.get("application_id"), jobRunId=conf.get("job_run_id")
        )
        return resp["url"]


class EmrServerlessS3LogsLink(BaseAwsLink):
    """Helper class for constructing link to S3 console for Amazon EMR Serverless Logs."""

    name = "S3 Logs"
    key = "emr_serverless_s3_logs"
    format_str = BASE_AWS_CONSOLE_LINK + (
        "/s3/buckets/{bucket_name}?region={region_name}"
        "&prefix={prefix}/applications/{application_id}/jobs/{job_run_id}/"
    )

    def format_link(self, **kwargs) -> str:
        bucket, prefix = S3Hook.parse_s3_url(kwargs["log_uri"])
        kwargs["bucket_name"] = bucket
        kwargs["prefix"] = prefix.rstrip("/")
        return super().format_link(**kwargs)


class EmrServerlessCloudWatchLogsLink(BaseAwsLink):
    """
    Helper class for constructing link to CloudWatch console for Amazon EMR Serverless Logs.
    
    This is a deep link that filters on a specific job run.
    """

    name = "CloudWatch Logs"
    key = "emr_serverless_cloudwatch_logs"
    format_str = (
        BASE_AWS_CONSOLE_LINK
        + "/cloudwatch/home?region={region_name}#logsV2:log-groups/log-group/{awslogs_group}{stream_prefix}"
    )

    def format_link(self, **kwargs) -> str:
        kwargs["awslogs_group"] = quote_plus(kwargs["awslogs_group"])
        kwargs["stream_prefix"] = quote_plus("?logStreamNameFilter=").replace("%", "$") + quote_plus(
            kwargs["stream_prefix"]
        )
        return super().format_link(**kwargs)
