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

from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.links.emr import (
    EmrClusterLink,
    EmrLogsLink,
    EmrServerlessCloudWatchLogsLink,
    EmrServerlessDashboardLink,
    EmrServerlessLogsLink,
    EmrServerlessS3LogsLink,
    get_log_uri,
    get_serverless_dashboard_url,
)

from providers.tests.amazon.aws.links.test_base_aws import BaseAwsLinksTestCase


class TestEmrClusterLink(BaseAwsLinksTestCase):
    link_class = EmrClusterLink

    def test_extra_link(self):
        self.assert_extra_link_url(
            expected_url=(
                "https://console.aws.amazon.com/emr/home?region=us-west-1#/clusterDetails/j-TEST-FLOW-ID"
            ),
            region_name="us-west-1",
            aws_partition="aws",
            job_flow_id="j-TEST-FLOW-ID",
        )


@pytest.mark.parametrize(
    "cluster_info, expected_uri",
    [
        pytest.param({"Cluster": {}}, None, id="no-log-uri"),
        pytest.param({"Cluster": {"LogUri": "s3://myLogUri/"}}, "myLogUri/", id="has-log-uri"),
    ],
)
def test_get_log_uri(cluster_info, expected_uri):
    emr_client = MagicMock()
    emr_client.describe_cluster.return_value = cluster_info
    assert get_log_uri(cluster=None, emr_client=emr_client, job_flow_id="test_job_flow_id") == expected_uri


class TestEmrLogsLink(BaseAwsLinksTestCase):
    link_class = EmrLogsLink

    def test_extra_link(self):
        self.assert_extra_link_url(
            expected_url=(
                "https://console.aws.amazon.com/s3/buckets/myLogUri/?region=eu-west-2&prefix=j-8989898989/"
            ),
            region_name="eu-west-2",
            aws_partition="aws",
            log_uri="myLogUri/",
            job_flow_id="j-8989898989",
        )

    @pytest.mark.parametrize(
        "log_url_extra",
        [
            pytest.param({}, id="no-log-uri"),
            pytest.param({"log_uri": None}, id="log-uri-none"),
            pytest.param({"log_uri": ""}, id="log-uri-empty"),
        ],
    )
    def test_missing_log_url(self, log_url_extra: dict):
        self.assert_extra_link_url(expected_url="", **log_url_extra)


@pytest.fixture
def mocked_emr_serverless_hook():
    with mock.patch("airflow.providers.amazon.aws.links.emr.EmrServerlessHook") as m:
        yield m


class TestEmrServerlessLogsLink(BaseAwsLinksTestCase):
    link_class = EmrServerlessLogsLink

    def test_extra_link(self, mocked_emr_serverless_hook):
        mocked_client = mocked_emr_serverless_hook.return_value.conn
        mocked_client.get_dashboard_for_job_run.return_value = {"url": "https://example.com/?authToken=1234"}

        self.assert_extra_link_url(
            expected_url="https://example.com/logs/SPARK_DRIVER/stdout.gz?authToken=1234",
            conn_id="aws-test",
            application_id="app-id",
            job_run_id="job-run-id",
        )

        mocked_emr_serverless_hook.assert_called_with(
            aws_conn_id="aws-test", config={"retries": {"total_max_attempts": 1}}
        )
        mocked_client.get_dashboard_for_job_run.assert_called_with(
            applicationId="app-id",
            jobRunId="job-run-id",
        )


class TestEmrServerlessDashboardLink(BaseAwsLinksTestCase):
    link_class = EmrServerlessDashboardLink

    def test_extra_link(self, mocked_emr_serverless_hook):
        mocked_client = mocked_emr_serverless_hook.return_value.conn
        mocked_client.get_dashboard_for_job_run.return_value = {"url": "https://example.com/?authToken=1234"}

        self.assert_extra_link_url(
            expected_url="https://example.com/?authToken=1234",
            conn_id="aws-test",
            application_id="app-id",
            job_run_id="job-run-id",
        )

        mocked_emr_serverless_hook.assert_called_with(
            aws_conn_id="aws-test", config={"retries": {"total_max_attempts": 1}}
        )
        mocked_client.get_dashboard_for_job_run.assert_called_with(
            applicationId="app-id",
            jobRunId="job-run-id",
        )


@pytest.mark.parametrize(
    "dashboard_info, expected_uri",
    [
        pytest.param(
            {"url": "https://example.com/?authToken=first-unique-value"},
            "https://example.com/?authToken=first-unique-value",
            id="first-call",
        ),
        pytest.param(
            {"url": "https://example.com/?authToken=second-unique-value"},
            "https://example.com/?authToken=second-unique-value",
            id="second-call",
        ),
    ],
)
def test_get_serverless_dashboard_url_with_client(mocked_emr_serverless_hook, dashboard_info, expected_uri):
    mocked_client = mocked_emr_serverless_hook.return_value.conn
    mocked_client.get_dashboard_for_job_run.return_value = dashboard_info

    url = get_serverless_dashboard_url(
        emr_serverless_client=mocked_client, application_id="anything", job_run_id="anything"
    )
    assert url
    assert url.geturl() == expected_uri
    mocked_emr_serverless_hook.assert_not_called()
    mocked_client.get_dashboard_for_job_run.assert_called_with(
        applicationId="anything",
        jobRunId="anything",
    )


def test_get_serverless_dashboard_url_with_conn_id(mocked_emr_serverless_hook):
    mocked_client = mocked_emr_serverless_hook.return_value.conn
    mocked_client.get_dashboard_for_job_run.return_value = {
        "url": "https://example.com/?authToken=some-unique-value"
    }

    url = get_serverless_dashboard_url(
        aws_conn_id="aws-test", application_id="anything", job_run_id="anything"
    )
    assert url
    assert url.geturl() == "https://example.com/?authToken=some-unique-value"
    mocked_emr_serverless_hook.assert_called_with(
        aws_conn_id="aws-test", config={"retries": {"total_max_attempts": 1}}
    )
    mocked_client.get_dashboard_for_job_run.assert_called_with(
        applicationId="anything",
        jobRunId="anything",
    )


def test_get_serverless_dashboard_url_parameters():
    with pytest.raises(
        AirflowException, match="Requires either an AWS connection ID or an EMR Serverless Client"
    ):
        get_serverless_dashboard_url(application_id="anything", job_run_id="anything")

    with pytest.raises(
        AirflowException, match="Requires either an AWS connection ID or an EMR Serverless Client"
    ):
        get_serverless_dashboard_url(
            aws_conn_id="a", emr_serverless_client="b", application_id="anything", job_run_id="anything"
        )


class TestEmrServerlessS3LogsLink(BaseAwsLinksTestCase):
    link_class = EmrServerlessS3LogsLink

    def test_extra_link(self):
        self.assert_extra_link_url(
            expected_url=(
                "https://console.aws.amazon.com/s3/buckets/bucket-name?region=us-west-1&prefix=logs/applications/app-id/jobs/job-run-id/"
            ),
            region_name="us-west-1",
            aws_partition="aws",
            log_uri="s3://bucket-name/logs/",
            application_id="app-id",
            job_run_id="job-run-id",
        )


class TestEmrServerlessCloudWatchLogsLink(BaseAwsLinksTestCase):
    link_class = EmrServerlessCloudWatchLogsLink

    def test_extra_link(self):
        self.assert_extra_link_url(
            expected_url=(
                "https://console.aws.amazon.com/cloudwatch/home?region=us-west-1#logsV2:log-groups/log-group/%2Faws%2Femrs$3FlogStreamNameFilter$3Dsome-prefix"
            ),
            region_name="us-west-1",
            aws_partition="aws",
            awslogs_group="/aws/emrs",
            stream_prefix="some-prefix",
            application_id="app-id",
            job_run_id="job-run-id",
        )
