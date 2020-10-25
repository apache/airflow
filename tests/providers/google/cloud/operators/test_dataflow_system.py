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
import json
import os
import shlex
import textwrap
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

import pytest
import requests

from airflow.providers.google.cloud.example_dags.example_dataflow_flex_template import (
    BQ_FLEX_TEMPLATE_DATASET,
    BQ_FLEX_TEMPLATE_LOCATION,
    DATAFLOW_FLEX_TEMPLATE_JOB_NAME,
    GCS_FLEX_TEMPLATE_TEMPLATE_PATH,
    PUBSUB_FLEX_TEMPLATE_SUBSCRIPTION,
    PUBSUB_FLEX_TEMPLATE_TOPIC,
)
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_DATAFLOW_KEY, GCP_GCS_TRANSFER_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_DATAFLOW_KEY)
class CloudDataflowExampleDagsSystemTest(GoogleSystemTest):
    @provide_gcp_context(GCP_DATAFLOW_KEY)
    def test_run_example_gcp_dataflow_native_java(self):
        self.run_dag('example_gcp_dataflow_native_java', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_DATAFLOW_KEY)
    def test_run_example_gcp_dataflow_native_python(self):
        self.run_dag('example_gcp_dataflow_native_python', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_DATAFLOW_KEY)
    def test_run_example_gcp_dataflow_template(self):
        self.run_dag('example_gcp_dataflow_template', CLOUD_DAG_FOLDER)


GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
GCR_FLEX_TEMPLATE_IMAGE = f"gcr.io/{GCP_PROJECT_ID}/samples-dataflow-streaming-beam-sql:latest"

# https://github.com/GoogleCloudPlatform/java-docs-samples/tree/954553c/dataflow/flex-templates/streaming_beam_sql
GCS_TEMPLATE_PARTS = urlparse(GCS_FLEX_TEMPLATE_TEMPLATE_PATH)
GCS_FLEX_TEMPLATE_BUCKET_NAME = GCS_TEMPLATE_PARTS.netloc


EXAMPLE_FLEX_TEMPLATE_REPO = "GoogleCloudPlatform/java-docs-samples"
EXAMPLE_FLEX_TEMPLATE_COMMIT = "deb0745be1d1ac1d133e1f0a7faa9413dbfbe5fe"
EXAMPLE_FLEX_TEMPLATE_SUBDIR = "dataflow/flex-templates/streaming_beam_sql"


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_GCS_TRANSFER_KEY)
class CloudDataflowExampleDagFlexTemplateJavagSystemTest(GoogleSystemTest):
    @provide_gcp_context(GCP_GCS_TRANSFER_KEY, project_id=GoogleSystemTest._project_id())
    def setUp(self) -> None:
        # Create a Cloud Storage bucket
        self.execute_cmd(["gsutil", "mb", f"gs://{GCS_FLEX_TEMPLATE_BUCKET_NAME}"])

        # Build image with pipeline
        with NamedTemporaryFile("w") as f:
            cloud_build_config = {
                'steps': [
                    {'name': 'gcr.io/cloud-builders/git', 'args': ['clone', "$_EXAMPLE_REPO", "repo_dir"]},
                    {
                        'name': 'gcr.io/cloud-builders/git',
                        'args': ['checkout', '$_EXAMPLE_COMMIT'],
                        'dir': 'repo_dir',
                    },
                    {
                        'name': 'maven',
                        'args': ['mvn', 'clean', 'package'],
                        'dir': 'repo_dir/$_EXAMPLE_SUBDIR',
                    },
                    {
                        'name': 'gcr.io/cloud-builders/docker',
                        'args': ['build', '-t', '$_TEMPLATE_IMAGE', '.'],
                        'dir': 'repo_dir/$_EXAMPLE_SUBDIR',
                    },
                ],
                'images': ['$_TEMPLATE_IMAGE'],
            }
            f.write(json.dumps(cloud_build_config))
            f.flush()
            self.execute_cmd(["cat", f.name])
            substitutions = {
                "_TEMPLATE_IMAGE": GCR_FLEX_TEMPLATE_IMAGE,
                "_EXAMPLE_REPO": f"https://github.com/{EXAMPLE_FLEX_TEMPLATE_REPO}.git",
                "_EXAMPLE_SUBDIR": EXAMPLE_FLEX_TEMPLATE_SUBDIR,
                "_EXAMPLE_COMMIT": EXAMPLE_FLEX_TEMPLATE_COMMIT,
            }
            self.execute_cmd(
                [
                    "gcloud",
                    "builds",
                    "submit",
                    "--substitutions="
                    + ",".join([f"{k}={shlex.quote(v)}" for k, v in substitutions.items()]),
                    f"--config={f.name}",
                    "--no-source",
                ]
            )

        # Build template
        with NamedTemporaryFile() as f:  # type: ignore
            manifest_url = (
                f"https://raw.githubusercontent.com/"
                f"{EXAMPLE_FLEX_TEMPLATE_REPO}/{EXAMPLE_FLEX_TEMPLATE_COMMIT}/"
                f"{EXAMPLE_FLEX_TEMPLATE_SUBDIR}/metadata.json"
            )
            f.write(requests.get(manifest_url).content)  # type: ignore
            f.flush()
            self.execute_cmd(
                [
                    "gcloud",
                    "beta",
                    "dataflow",
                    "flex-template",
                    "build",
                    GCS_FLEX_TEMPLATE_TEMPLATE_PATH,
                    "--image",
                    GCR_FLEX_TEMPLATE_IMAGE,
                    "--sdk-language",
                    "JAVA",
                    "--metadata-file",
                    f.name,
                ]
            )

        # Create a Pub/Sub topic and a subscription to that topic
        self.execute_cmd(["gcloud", "pubsub", "topics", "create", PUBSUB_FLEX_TEMPLATE_TOPIC])
        self.execute_cmd(
            [
                "gcloud",
                "pubsub",
                "subscriptions",
                "create",
                "--topic",
                PUBSUB_FLEX_TEMPLATE_TOPIC,
                PUBSUB_FLEX_TEMPLATE_SUBSCRIPTION,
            ]
        )
        # Create a publisher for "positive ratings" that publishes 1 message per minute
        self.execute_cmd(
            [
                "gcloud",
                "scheduler",
                "jobs",
                "create",
                "pubsub",
                "positive-ratings-publisher",
                '--schedule=* * * * *',
                f"--topic={PUBSUB_FLEX_TEMPLATE_TOPIC}",
                '--message-body=\'{"url": "https://beam.apache.org/", "review": "positive"}\'',
            ]
        )
        # Create and run another similar publisher for "negative ratings" that
        self.execute_cmd(
            [
                "gcloud",
                "scheduler",
                "jobs",
                "create",
                "pubsub",
                "negative-ratings-publisher",
                '--schedule=*/2 * * * *',
                f"--topic={PUBSUB_FLEX_TEMPLATE_TOPIC}",
                '--message-body=\'{"url": "https://beam.apache.org/", "review": "negative"}\'',
            ]
        )

        # Create a BigQuery dataset
        self.execute_cmd(["bq", "mk", "--dataset", f'{self._project_id()}:{BQ_FLEX_TEMPLATE_DATASET}'])

    @provide_gcp_context(GCP_GCS_TRANSFER_KEY)
    def test_run_example_dag_function(self):
        self.run_dag("example_gcp_dataflow_flex_template_java", CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_GCS_TRANSFER_KEY, project_id=GoogleSystemTest._project_id())
    def tearDown(self) -> None:
        # Stop the Dataflow pipeline.
        self.execute_cmd(
            [
                "bash",
                "-c",
                textwrap.dedent(
                    f"""\
                        gcloud dataflow jobs list \
                            --region={BQ_FLEX_TEMPLATE_LOCATION} \
                            --filter 'NAME:{DATAFLOW_FLEX_TEMPLATE_JOB_NAME} AND STATE=Running' \
                            --format 'value(JOB_ID)' \
                          | xargs -r gcloud dataflow jobs cancel --region={BQ_FLEX_TEMPLATE_LOCATION}
                    """
                ),
            ]
        )

        # Delete the template spec file from Cloud Storage
        self.execute_cmd(["gsutil", "rm", GCS_FLEX_TEMPLATE_TEMPLATE_PATH])

        # Delete the Flex Template container image from Container Registry.
        self.execute_cmd(
            [
                "gcloud",
                "container",
                "images",
                "delete",
                GCR_FLEX_TEMPLATE_IMAGE,
                "--force-delete-tags",
                "--quiet",
            ]
        )

        # Delete the Cloud Scheduler jobs.
        self.execute_cmd(["gcloud", "scheduler", "jobs", "delete", "negative-ratings-publisher", "--quiet"])
        self.execute_cmd(["gcloud", "scheduler", "jobs", "delete", "positive-ratings-publisher", "--quiet"])

        # Delete the Pub/Sub subscription and topic.
        self.execute_cmd(["gcloud", "pubsub", "subscriptions", "delete", PUBSUB_FLEX_TEMPLATE_SUBSCRIPTION])
        self.execute_cmd(["gcloud", "pubsub", "topics", "delete", PUBSUB_FLEX_TEMPLATE_TOPIC])

        # Delete the BigQuery dataset,
        self.execute_cmd(["bq", "rm", "-r", "-f", "-d", f'{self._project_id()}:{BQ_FLEX_TEMPLATE_DATASET}'])

        # Delete the Cloud Storage bucket
        self.execute_cmd(["gsutil", "rm", "-r", f"gs://{GCS_FLEX_TEMPLATE_BUCKET_NAME}"])
