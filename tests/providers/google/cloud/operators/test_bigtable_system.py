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
import os

import pytest

from tests.providers.google.cloud.utils.gcp_authenticator import GCP_BIGTABLE_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
CBT_INSTANCE = os.environ.get('CBT_INSTANCE_ID', 'testinstance')


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_BIGTABLE_KEY)
class BigTableExampleDagsSystemTest(GoogleSystemTest):

    @provide_gcp_context(GCP_BIGTABLE_KEY)
    def test_run_example_dag_gcs_bigtable(self):
        self.run_dag('example_gcp_bigtable_operators', CLOUD_DAG_FOLDER)

    @provide_gcp_context(GCP_BIGTABLE_KEY)
    def tearDown(self):
        self.execute_with_ctx([
            'gcloud', 'bigtable', '--project', GCP_PROJECT_ID,
            '--quiet', '--verbosity=none',
            'instances', 'delete', CBT_INSTANCE
        ], key=GCP_BIGTABLE_KEY)
        super().tearDown()
