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
import pytest

from airflow.providers.amazon.aws.example_dags.example_imap_attachment_to_s3 import S3_DESTINATION_KEY
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tests.test_utils.amazon_system_helpers import (
    AWS_DAG_FOLDER, AmazonSystemTest, provide_aws_context, provide_aws_s3_bucket,
)

BUCKET, _ = S3Hook.parse_s3_url(S3_DESTINATION_KEY)


@pytest.fixture
def provide_s3_bucket():
    with provide_aws_s3_bucket(BUCKET):
        yield


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.system("imap")
class TestImapAttachmentToS3ExampleDags(AmazonSystemTest):

    @pytest.mark.usefixtures("provide_s3_bucket")
    @provide_aws_context()
    def test_run_example_dag_imap_attachment_to_s3(self):
        self.run_dag('example_imap_attachment_to_s3', AWS_DAG_FOLDER)
