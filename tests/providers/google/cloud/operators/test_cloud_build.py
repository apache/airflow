# -*- coding: utf-8 -*-
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
"""Tests for Google Cloud Build operators """

from copy import deepcopy
from unittest import TestCase

import mock
from parameterized import parameterized

from airflow import AirflowException
from airflow.providers.google.cloud.operators.cloud_build import BuildProcessor, CloudBuildCreateOperator

TEST_CREATE_BODY = {
    "source": {"storageSource": {"bucket": "cloud-build-examples", "object": "node-docker-example.tar.gz"}},
    "steps": [
        {"name": "gcr.io/cloud-builders/docker", "args": ["build", "-t", "gcr.io/$PROJECT_ID/my-image", "."]}
    ],
    "images": ["gcr.io/$PROJECT_ID/my-image"],
}
TEST_PROJECT_ID = "example-id"


class TestBuildProcessor(TestCase):
    def test_verify_source(self):
        with self.assertRaisesRegex(AirflowException, "The source could not be determined."):
            BuildProcessor(body={"source": {"storageSource": {}, "repoSource": {}}}).process_body()

    @parameterized.expand(
        [
            (
                "https://source.developers.google.com/p/airflow-project/r/airflow-repo",
                {"projectId": "airflow-project", "repoName": "airflow-repo", "branchName": "master"},
            ),
            (
                "https://source.developers.google.com/p/airflow-project/r/airflow-repo#branch-name",
                {"projectId": "airflow-project", "repoName": "airflow-repo", "branchName": "branch-name"},
            ),
            (
                "https://source.developers.google.com/p/airflow-project/r/airflow-repo#feature/branch",
                {"projectId": "airflow-project", "repoName": "airflow-repo", "branchName": "feature/branch"},
            ),
        ]
    )
    def test_convert_repo_url_to_dict_valid(self, url, expected_dict):
        body = {"source": {"repoSource": url}}
        body = BuildProcessor(body=body).process_body()
        self.assertEqual(body["source"]["repoSource"], expected_dict)

    @parameterized.expand(
        [
            ("https://source.e.com/p/airflow-project/r/airflow-repo#branch-name",),
            ("httpXs://source.developers.google.com/p/airflow-project/r/airflow-repo",),
            ("://source.developers.google.com/p/airflow-project/r/airflow-repo",),
            ("://source.developers.google.com/p/airflow-project/rXXXX/airflow-repo",),
            ("://source.developers.google.com/pXXX/airflow-project/r/airflow-repo",),
        ]
    )
    def test_convert_repo_url_to_storage_dict_invalid(self, url):
        body = {"source": {"repoSource": url}}
        with self.assertRaisesRegex(AirflowException, "Invalid URL."):
            BuildProcessor(body=body).process_body()

    @parameterized.expand(
        [
            (
                "gs://bucket-name/airflow-object.tar.gz",
                {"bucket": "bucket-name", "object": "airflow-object.tar.gz"},
            ),
            (
                "gs://bucket-name/airflow-object.tar.gz#1231231",
                {"bucket": "bucket-name", "object": "airflow-object.tar.gz", "generation": "1231231"},
            ),
        ]
    )
    def test_convert_storage_url_to_dict_valid(self, url, expected_dict):
        body = {"source": {"storageSource": url}}
        body = BuildProcessor(body=body).process_body()
        self.assertEqual(body["source"]["storageSource"], expected_dict)

    @parameterized.expand(
        [("///object",), ("gsXXa:///object",), ("gs://bucket-name/",), ("gs://bucket-name",)]
    )
    def test_convert_storage_url_to_dict_invalid(self, url):
        body = {"source": {"storageSource": url}}
        with self.assertRaisesRegex(AirflowException, "Invalid URL."):
            BuildProcessor(body=body).process_body()

    @parameterized.expand([("storageSource",), ("repoSource",)])
    def test_do_nothing(self, source_key):
        body = {"source": {source_key: {}}}
        expected_body = deepcopy(body)

        BuildProcessor(body=body).process_body()
        self.assertEqual(body, expected_body)


class TestGcpCloudBuildCreateOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.create_build.return_value = TEST_CREATE_BODY
        operator = CloudBuildCreateOperator(
            body=TEST_CREATE_BODY, project_id=TEST_PROJECT_ID, task_id="task-id"
        )
        result = operator.execute({})
        self.assertIs(result, TEST_CREATE_BODY)

    @parameterized.expand([({},), (None,)])
    def test_missing_input(self, body):
        with self.assertRaisesRegex(AirflowException, "The required parameter 'body' is missing"):
            CloudBuildCreateOperator(body=body, project_id=TEST_PROJECT_ID, task_id="task-id")

    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_storage_source_replace(self, hook_mock):
        hook_mock.return_value.create_build.return_value = TEST_CREATE_BODY
        current_body = {
            # [START howto_operator_gcp_cloud_build_source_gcs_url]
            "source": {"storageSource": "gs://bucket-name/object-name.tar.gz"},
            # [END howto_operator_gcp_cloud_build_source_gcs_url]
            "steps": [
                {
                    "name": "gcr.io/cloud-builders/docker",
                    "args": ["build", "-t", "gcr.io/$PROJECT_ID/docker-image", "."],
                }
            ],
            "images": ["gcr.io/$PROJECT_ID/docker-image"],
        }

        operator = CloudBuildCreateOperator(
            body=current_body, project_id=TEST_PROJECT_ID, task_id="task-id"
        )
        operator.execute({})

        expected_result = {
            # [START howto_operator_gcp_cloud_build_source_gcs_dict]
            "source": {"storageSource": {"bucket": "bucket-name", "object": "object-name.tar.gz"}},
            # [END howto_operator_gcp_cloud_build_source_gcs_dict]
            "steps": [
                {
                    "name": "gcr.io/cloud-builders/docker",
                    "args": ["build", "-t", "gcr.io/$PROJECT_ID/docker-image", "."],
                }
            ],
            "images": ["gcr.io/$PROJECT_ID/docker-image"],
        }
        hook_mock.create_build(body=expected_result, project_id=TEST_PROJECT_ID)

    @mock.patch(
        "airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook",
    )
    def test_repo_source_replace(self, hook_mock):
        hook_mock.return_value.create_build.return_value = TEST_CREATE_BODY
        current_body = {
            # [START howto_operator_gcp_cloud_build_source_repo_url]
            "source": {"repoSource": "https://source.developers.google.com/p/airflow-project/r/airflow-repo"},
            # [END howto_operator_gcp_cloud_build_source_repo_url]
            "steps": [
                {
                    "name": "gcr.io/cloud-builders/docker",
                    "args": ["build", "-t", "gcr.io/$PROJECT_ID/docker-image", "."],
                }
            ],
            "images": ["gcr.io/$PROJECT_ID/docker-image"],
        }
        operator = CloudBuildCreateOperator(
            body=current_body, project_id=TEST_PROJECT_ID, task_id="task-id"
        )
        return_value = operator.execute({})
        expected_body = {
            # [START howto_operator_gcp_cloud_build_source_repo_dict]
            "source": {
                "repoSource": {
                    "projectId": "airflow-project",
                    "repoName": "airflow-repo",
                    "branchName": "master",
                }
            },
            # [END howto_operator_gcp_cloud_build_source_repo_dict]
            "steps": [
                {
                    "name": "gcr.io/cloud-builders/docker",
                    "args": ["build", "-t", "gcr.io/$PROJECT_ID/docker-image", "."],
                }
            ],
            "images": ["gcr.io/$PROJECT_ID/docker-image"],
        }
        hook_mock.return_value.create_build.assert_called_once_with(
            body=expected_body, project_id=TEST_PROJECT_ID
        )
        self.assertEqual(return_value, TEST_CREATE_BODY)
