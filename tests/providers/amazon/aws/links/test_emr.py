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

from unittest.mock import MagicMock

import pytest

from airflow.providers.amazon.aws.links.emr import EmrClusterLink, EmrLogsLink, get_log_uri
from tests.providers.amazon.aws.links.test_base_aws import BaseAwsLinksTestCase


class TestEmrClusterLink(BaseAwsLinksTestCase):
    link_class = EmrClusterLink

    def test_extra_link(self):
        self.assert_extra_link_url(
            expected_url=(
                "https://console.aws.amazon.com/emr/home" "?region=us-west-1#/clusterDetails/j-TEST-FLOW-ID"
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
                "https://console.aws.amazon.com/s3/buckets/myLogUri/" "?region=eu-west-2&prefix=j-8989898989/"
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
