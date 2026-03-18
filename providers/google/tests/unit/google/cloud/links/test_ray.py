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

from unittest import mock

from airflow.providers.google.cloud.links.ray import RayJobLink

TEST_CLUSTER_ADDRESS = "ray-head-123.us-central1.ray.googleusercontent.com"
TEST_JOB_ID = "test-job-id"

EXPECTED_RAY_JOB_LINK_NAME = "Ray Job"
EXPECTED_RAY_JOB_LINK_KEY = "ray_job"
EXPECTED_RAY_JOB_LINK_FORMAT_STR = "http://{cluster_address}/#/jobs/{job_id}"


class TestRayJobLink:
    def test_class_attributes(self):
        assert RayJobLink.key == EXPECTED_RAY_JOB_LINK_KEY
        assert RayJobLink.name == EXPECTED_RAY_JOB_LINK_NAME
        assert RayJobLink.format_str == EXPECTED_RAY_JOB_LINK_FORMAT_STR

    def test_persist(self):
        mock_context = mock.MagicMock()
        mock_context["ti"] = mock.MagicMock()
        mock_context["task"] = mock.MagicMock()

        RayJobLink.persist(
            context=mock_context,
            cluster_address=TEST_CLUSTER_ADDRESS,
            job_id=TEST_JOB_ID,
        )

        mock_context["ti"].xcom_push.assert_called_once_with(
            key=EXPECTED_RAY_JOB_LINK_KEY,
            value={
                "cluster_address": TEST_CLUSTER_ADDRESS,
                "job_id": TEST_JOB_ID,
            },
        )
