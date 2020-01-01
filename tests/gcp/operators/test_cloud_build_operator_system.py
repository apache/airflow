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
"""System tests for Google Cloud Build operators"""
import os
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor
from tests.gcp.utils.gcp_authenticator import GCP_CLOUD_BUILD_KEY
from tests.test_utils.gcp_system_helpers import GCP_DAG_FOLDER, provide_gcp_context, skip_gcp_system
from tests.test_utils.system_tests_class import SystemTest

GCE_INSTANCE = os.environ.get("GCE_INSTANCE", "testinstance")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
GCP_ARCHIVE_URL = os.environ.get("GCP_CLOUD_BUILD_ARCHIVE_URL", "gs://example-bucket/source-code.tar.gz")
GCP_ARCHIVE_URL_PARTS = urlparse(GCP_ARCHIVE_URL)
GCP_BUCKET_NAME = GCP_ARCHIVE_URL_PARTS.netloc
GCP_OBJECT_NAME = GCP_ARCHIVE_URL_PARTS.path[1:]

GCP_REPOSITORY_NAME = os.environ.get("GCP_CLOUD_BUILD_REPOSITORY_NAME", "repository-name")


@skip_gcp_system(GCP_CLOUD_BUILD_KEY)
class CloudBuildExampleDagsSystemTest(SystemTest, LoggingCommandExecutor):
    """
    System tests for Google Cloud Build operators

    It use a real service.
    """

    @provide_gcp_context(GCP_CLOUD_BUILD_KEY)
    def create_repository_and_bucket(self):
        """Create a bucket and a repository with sample application."""

        with TemporaryDirectory(prefix="airflow-gcp") as tmp_dir:
            # 1. Create required files
            quickstart_path = os.path.join(tmp_dir, "quickstart.sh")
            with open(quickstart_path, "w") as file:
                file.write("#!/bin/sh\n")
                file.write('echo "Hello, world! The time is $(date)."\n')
                file.flush()

            os.chmod(quickstart_path, 555)

            with open(os.path.join(tmp_dir, "Dockerfile"), "w") as file:
                file.write("FROM alpine\n")
                file.write("COPY quickstart.sh /\n")
                file.write('CMD ["/quickstart.sh"]\n')
                file.flush()

            # 2. Prepare bucket
            self.execute_cmd(["gsutil", "mb", "gs://{}".format(GCP_BUCKET_NAME)])
            self.execute_cmd(
                ["bash", "-c", "tar -zcvf - -C {} . | gsutil cp -r - {}".format(tmp_dir, GCP_ARCHIVE_URL)]
            )

            # 3. Prepare repo
            self.execute_cmd(["gcloud", "source", "repos", "create", GCP_REPOSITORY_NAME])
            self.execute_cmd(["git", "init"], cwd=tmp_dir)
            self.execute_cmd(["git", "config", "user.email", "bot@example.com"], cwd=tmp_dir)
            self.execute_cmd(["git", "config", "user.name", "system-test"])
            self.execute_cmd(
                ["git", "config", "credential.https://source.developers.google.com.helper", "gcloud.sh"],
                cwd=tmp_dir,
            )
            self.execute_cmd(["git", "add", "."], cwd=tmp_dir)
            self.execute_cmd(["git", "commit", "-m", "Initial commit"], cwd=tmp_dir)
            repo_url = "https://source.developers.google.com/p/{}/r/{}".format(
                GCP_PROJECT_ID, GCP_REPOSITORY_NAME
            )
            self.execute_cmd(["git", "remote", "add", "origin", repo_url], cwd=tmp_dir)
            self.execute_cmd(["git", "push", "origin", "master"], cwd=tmp_dir)

    @provide_gcp_context(GCP_CLOUD_BUILD_KEY)
    def delete_repo(self):
        """Delete repository in Google Cloud Source Repository service"""

        self.execute_cmd(["gcloud", "source", "repos", "delete", GCP_REPOSITORY_NAME, "--quiet"])

    @provide_gcp_context(GCP_CLOUD_BUILD_KEY)
    def delete_bucket(self):
        """Delete bucket in Google Cloud Storage service"""

        self.execute_cmd(["gsutil", "rb", "gs://{}".format(GCP_BUCKET_NAME)])

    @provide_gcp_context(GCP_CLOUD_BUILD_KEY)
    def delete_docker_images(self):
        """Delete images in Google Cloud Container Registry"""
        repo_image_name = "gcr.io/{}/{}".format(GCP_PROJECT_ID, GCP_REPOSITORY_NAME)
        self.execute_cmd(["gcloud", "container", "images", "delete", repo_image_name])
        bucket_image_name = "gcr.io/{}/{}".format(GCP_PROJECT_ID, GCP_BUCKET_NAME)
        self.execute_cmd(["gcloud", "container", "images", "delete", bucket_image_name])

    @provide_gcp_context(GCP_CLOUD_BUILD_KEY)
    def setUp(self):
        super().setUp()
        self.create_repository_and_bucket()

    @provide_gcp_context(GCP_CLOUD_BUILD_KEY)
    def test_run_example_dag(self):
        self.run_dag("example_gcp_cloud_build", GCP_DAG_FOLDER)

    @provide_gcp_context(GCP_CLOUD_BUILD_KEY)
    def tearDown(self):
        self.delete_bucket()
        self.delete_docker_images()
        self.delete_repo()
        super().tearDown()


if __name__ == "__main__":
    CloudBuildExampleDagsSystemTest().cli()
