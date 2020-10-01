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

from tests.test_utils.amazon_system_helpers import AWS_DAG_FOLDER, AmazonSystemTest
from tests.test_utils.gcp_system_helpers import GoogleSystemTest


BUCKET = "data_from_glacier"


class GlacierSystemTest(AmazonSystemTest):
    """
    System test for AWS Glacier operators
    """

    def setUp(self):
        super().setUp()
        GoogleSystemTest.create_gcs_bucket(BUCKET)

    def tearDown(self):
        GoogleSystemTest.delete_gcs_bucket(BUCKET)  # pylint: disable=no-member

    def test_run_example_dag(self):
        self.run_dag(dag_id="example_glacier_to_gcs", dag_folder=AWS_DAG_FOLDER)
