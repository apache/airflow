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

import tempfile
from copy import deepcopy
from datetime import datetime
from unittest import TestCase, mock

import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.models import DAG, TaskInstance
from airflow.providers.google.cloud.operators.cloud_build import BuildProcessor, CloudBuildCreateBuildOperator

TEST_CREATE_BODY = {
    "source": {"storageSource": {"bucket": "cloud-build-examples", "object": "node-docker-example.tar.gz"}},
    "steps": [
        {"name": "gcr.io/cloud-builders/docker", "args": ["build", "-t", "gcr.io/$PROJECT_ID/my-image", "."]}
    ],
    "images": ["gcr.io/$PROJECT_ID/my-image"],
}
TEST_PROJECT_ID = "example-id"
TEST_DEFAULT_DATE = datetime(year=2020, month=1, day=1)


class TestBuildProcessor(TestCase):
    def test_verify_source(self):
        with pytest.raises(AirflowException, match="The source could not be determined."):
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
        assert body["source"]["repoSource"] == expected_dict

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
        with pytest.raises(AirflowException, match="Invalid URL."):
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
        assert body["source"]["storageSource"] == expected_dict

    @parameterized.expand(
        [("///object",), ("gsXXa:///object",), ("gs://bucket-name/",), ("gs://bucket-name",)]
    )
    def test_convert_storage_url_to_dict_invalid(self, url):
        body = {"source": {"storageSource": url}}
        with pytest.raises(AirflowException, match="Invalid URL."):
            BuildProcessor(body=body).process_body()

    @parameterized.expand([("storageSource",), ("repoSource",)])
    def test_do_nothing(self, source_key):
        body = {"source": {source_key: {}}}
        expected_body = deepcopy(body)

        BuildProcessor(body=body).process_body()
        assert body == expected_body


class TestGcpCloudBuildCreateBuildOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.cloud_build.CloudBuildHook")
    def test_minimal_green_path(self, mock_hook):
        mock_hook.return_value.create_build.return_value = TEST_CREATE_BODY
        operator = CloudBuildCreateBuildOperator(
            body=TEST_CREATE_BODY, project_id=TEST_PROJECT_ID, task_id="task-id"
        )
        result = operator.execute({})
        assert result is TEST_CREATE_BODY

    @parameterized.expand([({},), (None,)])
    def test_missing_input(self, body):
        with pytest.raises(AirflowException, match="The required parameter 'body' is missing"):
            CloudBuildCreateBuildOperator(body=body, project_id=TEST_PROJECT_ID, task_id="task-id")

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

        operator = CloudBuildCreateBuildOperator(
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
        operator = CloudBuildCreateBuildOperator(
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
        assert return_value == TEST_CREATE_BODY

    def test_load_templated_yaml(self):
        dag = DAG(dag_id='example_cloudbuild_operator', start_date=TEST_DEFAULT_DATE)
        with tempfile.NamedTemporaryFile(suffix='.yaml', mode='w+t') as build:
            build.writelines(
                """
            steps:
                - name: 'ubuntu'
                  args: ['echo', 'Hello {{ params.name }}!']
            """
            )
            build.seek(0)
            body_path = build.name
            operator = CloudBuildCreateBuildOperator(
                body=body_path, task_id="task-id", dag=dag, params={'name': 'airflow'}
            )
            operator.prepare_template()
            ti = TaskInstance(operator, TEST_DEFAULT_DATE)
            ti.render_templates()
            expected_body = {'steps': [{'name': 'ubuntu', 'args': ['echo', 'Hello airflow!']}]}
            assert expected_body == operator.body
