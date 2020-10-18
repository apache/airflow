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
#

import pytest

from airflow.providers.amazon.aws.example_dags.example_google_api_to_s3_transfer_advanced import (
    S3_DESTINATION_KEY as ADVANCED_S3_DESTINATION_KEY,
)
from airflow.providers.amazon.aws.example_dags.example_google_api_to_s3_transfer_basic import (
    S3_DESTINATION_KEY as BASIC_S3_DESTINATION_KEY,
)
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tests.providers.google.cloud.utils.gcp_authenticator import GMP_KEY
from tests.test_utils.amazon_system_helpers import (
    AWS_DAG_FOLDER,
    AmazonSystemTest,
    provide_aws_context,
    provide_aws_s3_bucket,
)
from tests.test_utils.gcp_system_helpers import GoogleSystemTest, provide_gcp_context

BASIC_BUCKET, _ = S3Hook.parse_s3_url(BASIC_S3_DESTINATION_KEY)
ADVANCED_BUCKET, _ = S3Hook.parse_s3_url(ADVANCED_S3_DESTINATION_KEY)


@pytest.fixture
def provide_s3_bucket_basic():
    with provide_aws_s3_bucket(BASIC_BUCKET):
        yield


@pytest.fixture
def provide_s3_bucket_advanced():
    with provide_aws_s3_bucket(ADVANCED_BUCKET):
        yield


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GMP_KEY)
class GoogleApiToS3TransferExampleDagsSystemTest(GoogleSystemTest, AmazonSystemTest):
    @pytest.mark.usefixtures("provide_s3_bucket_basic")
    @provide_aws_context()
    @provide_gcp_context(GMP_KEY, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
    def test_run_example_dag_google_api_to_s3_transfer_basic(self):
        self.run_dag('example_google_api_to_s3_transfer_basic', AWS_DAG_FOLDER)

    @pytest.mark.usefixtures("provide_s3_bucket_advanced")
    @provide_aws_context()
    @provide_gcp_context(GMP_KEY, scopes=['https://www.googleapis.com/auth/youtube.readonly'])
    def test_run_example_dag_google_api_to_s3_transfer_advanced(self):
        self.run_dag('example_google_api_to_s3_transfer_advanced', AWS_DAG_FOLDER)
