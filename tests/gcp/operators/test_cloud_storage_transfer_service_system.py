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

import pytest

from tests.gcp.operators.test_cloud_storage_transfer_service_system_helper import GCPTransferTestHelper
from tests.gcp.utils.gcp_authenticator import GCP_GCS_TRANSFER_KEY
from tests.test_utils.gcp_system_helpers import GCP_DAG_FOLDER, GcpSystemTest, provide_gcp_context

test_helper = GCPTransferTestHelper()


@pytest.fixture
def helper():
    test_helper.create_gcs_buckets()
    yield
    test_helper.delete_gcs_buckets()


@GcpSystemTest.skip(GCP_GCS_TRANSFER_KEY)
@pytest.mark.usefixtures("helper")
def test_run_example_dag_compute():
    with provide_gcp_context(GCP_GCS_TRANSFER_KEY):
        GcpSystemTest.run_dag("example_gcp_transfer", GCP_DAG_FOLDER)
