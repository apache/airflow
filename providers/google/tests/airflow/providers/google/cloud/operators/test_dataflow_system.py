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

import json
import os
import shlex
import textwrap
from tempfile import NamedTemporaryFile
from urllib.parse import urlsplit

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
from airflow.providers.google.cloud.example_dags.example_dataflow_sql import (
    BQ_SQL_DATASET,
    DATAFLOW_SQL_JOB_NAME,
    DATAFLOW_SQL_LOCATION,
)
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_DATAFLOW_KEY, GCP_GCS_TRANSFER_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_DATAFLOW_KEY)
class CloudDataflowExampleDagsSystemTest(GoogleSystemTest):
    @provide_gcp_context(GCP_DATAFLOW_KEY)
    def test_run_example_gcp_dataflow_native_java(self):
        self.run_dag("example_gcp_dataflow_native_java", CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_DATAFLOW_KEY)
    def test_run_example_gcp_dataflow_native_python(self):
        self.run_dag("example_gcp_dataflow_native_python", CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_DATAFLOW_KEY)
    def test_run_example_gcp_dataflow_native_python_async(self):
        self.run_dag("example_gcp_dataflow_native_python_async", CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_DATAFLOW_KEY)
    def test_run_example_gcp_dataflow_template(self):
        self.run_dag("example_gcp_dataflow_template", CLOUD_DAG_FOLDER)


GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
GCR_FLEX_TEMPLATE_IMAGE = f"gcr.io/{GCP_PROJECT_ID}/samples-dataflow-streaming-beam-sql:latest"

# https://github.com/GoogleCloudPlatform/java-docs-samples/tree/954553c/dataflow/flex-templates/streaming_beam_sql
GCS_TEMPLATE_PARTS = urlsplit(GCS_FLEX_TEMPLATE_TEMPLATE_PATH)
GCS_FLEX_TEMPLATE_BUCKET_NAME = GCS_TEMPLATE_PARTS.netloc


EXAMPLE_FLEX_TEMPLATE_REPO = "GoogleCloudPlatform/java-docs-samples"
EXAMPLE_FLEX_TEMPLATE_COMMIT = "deb0745be1d1ac1d133e1f0a7faa9413dbfbe5fe"
EXAMPLE_FLEX_TEMPLATE_SUBDIR = "dataflow/flex-templates/streaming_beam_sql"


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_GCS_TRANSFER_KEY)
class CloudDataflowExampleDagFlexTemplateJavagSystemTest(GoogleSystemTest):
    @provide_gcp_context(GCP_GCS_TRANSFER_KEY, project_id=GoogleSystemTest._project_id())
    def setUp(self) -> None:
        super().setUp()
        # Create a Cloud Storage bucket
        self.execute_cmd(["gsutil", "mb", f"gs://{GCS_FLEX_TEMPLATE_BUCKET_NAME}"])

        # Build image with pipeline
        with NamedTemporaryFile("w") as f:
            cloud_build_config = {
                "steps": [
                    {"name": "gcr.io/cloud-builders/git", "args": ["clone", "$_EXAMPLE_REPO", "repo_dir"]},
                    {
                        "name": "gcr.io/cloud-builders/git",
                        "args": ["checkout", "$_EXAMPLE_COMMIT"],
                        "dir": "repo_dir",
                    },
                    {
                        "name": "maven",
                        "args": ["mvn", "clean", "package"],
                        "dir": "repo_dir/$_EXAMPLE_SUBDIR",
                    },
                    {
                        "name": "gcr.io/cloud-builders/docker",
                        "args": ["build", "-t", "$_TEMPLATE_IMAGE", "."],
                        "dir": "repo_dir/$_EXAMPLE_SUBDIR",
                    },
                ],
                "images": ["$_TEMPLATE_IMAGE"],
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
                    "--substitutions=" + ",".join(f"{k}={shlex.quote(v)}" for k, v in substitutions.items()),
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
                "--schedule=* * * * *",
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
                "--schedule=*/2 * * * *",
                f"--topic={PUBSUB_FLEX_TEMPLATE_TOPIC}",
                '--message-body=\'{"url": "https://beam.apache.org/", "review": "negative"}\'',
            ]
        )

        # Create a BigQuery dataset
        self.execute_cmd(["bq", "mk", "--dataset", f"{self._project_id()}:{BQ_FLEX_TEMPLATE_DATASET}"])

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
        self.execute_cmd(["bq", "rm", "-r", "-f", "-d", f"{self._project_id()}:{BQ_FLEX_TEMPLATE_DATASET}"])

        # Delete the Cloud Storage bucket
        self.execute_cmd(["gsutil", "rm", "-r", f"gs://{GCS_FLEX_TEMPLATE_BUCKET_NAME}"])
        super().tearDown()


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_GCS_TRANSFER_KEY)
class CloudDataflowExampleDagSqlSystemTest(GoogleSystemTest):
    @provide_gcp_context(GCP_GCS_TRANSFER_KEY, project_id=GoogleSystemTest._project_id())
    def setUp(self) -> None:
        super().setUp()
        # Build image with pipeline
        with NamedTemporaryFile(suffix=".csv") as f:
            f.write(
                textwrap.dedent(
                    """\
                    state_id,state_code,state_name,sales_region
                    1,MO,Missouri,Region_1
                    2,SC,South Carolina,Region_1
                    3,IN,Indiana,Region_1
                    6,DE,Delaware,Region_2
                    15,VT,Vermont,Region_2
                    16,DC,District of Columbia,Region_2
                    19,CT,Connecticut,Region_2
                    20,ME,Maine,Region_2
                    35,PA,Pennsylvania,Region_2
                    38,NJ,New Jersey,Region_2
                    47,MA,Massachusetts,Region_2
                    54,RI,Rhode Island,Region_2
                    55,NY,New York,Region_2
                    60,MD,Maryland,Region_2
                    66,NH,New Hampshire,Region_2
                    4,CA,California,Region_3
                    8,AK,Alaska,Region_3
                    37,WA,Washington,Region_3
                    61,OR,Oregon,Region_3
                    33,HI,Hawaii,Region_4
                    59,AS,American Samoa,Region_4
                    65,GU,Guam,Region_4
                    5,IA,Iowa,Region_5
                    32,NV,Nevada,Region_5
                    11,PR,Puerto Rico,Region_6
                    17,CO,Colorado,Region_6
                    18,MS,Mississippi,Region_6
                    41,AL,Alabama,Region_6
                    42,AR,Arkansas,Region_6
                    43,FL,Florida,Region_6
                    44,NM,New Mexico,Region_6
                    46,GA,Georgia,Region_6
                    48,KS,Kansas,Region_6
                    52,AZ,Arizona,Region_6
                    56,TN,Tennessee,Region_6
                    58,TX,Texas,Region_6
                    63,LA,Louisiana,Region_6
                    7,ID,Idaho,Region_7
                    12,IL,Illinois,Region_7
                    13,ND,North Dakota,Region_7
                    31,MN,Minnesota,Region_7
                    34,MT,Montana,Region_7
                    36,SD,South Dakota,Region_7
                    50,MI,Michigan,Region_7
                    51,UT,Utah,Region_7
                    64,WY,Wyoming,Region_7
                    9,NE,Nebraska,Region_8
                    10,VA,Virginia,Region_8
                    14,OK,Oklahoma,Region_8
                    39,NC,North Carolina,Region_8
                    40,WV,West Virginia,Region_8
                    45,KY,Kentucky,Region_8
                    53,WI,Wisconsin,Region_8
                    57,OH,Ohio,Region_8
                    49,VI,United States Virgin Islands,Region_9
                    62,MP,Commonwealth of the Northern Mariana Islands,Region_9
                    """
                ).encode()
            )
            f.flush()

            self.execute_cmd(["bq", "mk", "--dataset", f"{self._project_id()}:{BQ_SQL_DATASET}"])

            self.execute_cmd(
                ["bq", "load", "--autodetect", "--source_format=CSV", f"{BQ_SQL_DATASET}.beam_input", f.name]
            )

    @provide_gcp_context(GCP_GCS_TRANSFER_KEY, project_id=GoogleSystemTest._project_id())
    def test_run_example_dag_function(self):
        self.run_dag("example_gcp_dataflow_sql", CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_GCS_TRANSFER_KEY, project_id=GoogleSystemTest._project_id())
    def tearDown(self) -> None:
        # Execute test query
        self.execute_cmd(
            [
                "bq",
                "query",
                "--use_legacy_sql=false",
                f"select * FROM `{self._project_id()}.{BQ_SQL_DATASET}.beam_output`",
            ]
        )

        # Stop the Dataflow pipelines.
        self.execute_cmd(
            [
                "bash",
                "-c",
                textwrap.dedent(
                    f"""\
                        gcloud dataflow jobs list \
                            --region={DATAFLOW_SQL_LOCATION} \
                            --filter 'NAME:{DATAFLOW_SQL_JOB_NAME} AND STATE=Running' \
                            --format 'value(JOB_ID)' \
                          | xargs -r gcloud dataflow jobs cancel --region={DATAFLOW_SQL_LOCATION}
                    """
                ),
            ]
        )
        # Delete the BigQuery dataset,
        self.execute_cmd(["bq", "rm", "-r", "-f", "-d", f"{self._project_id()}:{BQ_SQL_DATASET}"])
        super().tearDown()
