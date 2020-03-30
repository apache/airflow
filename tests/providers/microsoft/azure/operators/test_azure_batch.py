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
import unittest
import mock
import json

from airflow.models import Connection
from airflow.providers.microsoft.azure.operators.azure_batch import AzureBatchOperator
from airflow.utils import db

TASK_ID = "mydag"


class TestAzureBatchOperator(unittest.TestCase):
    # set up the test environment
    def setUp(self):
        # set up the test variable
        self.batch_pool_id = "mypool"
        self.batch_job_id = "myjob"
        self.batch_task_id = "mytask"
        self.vm_size = "standard"
        self.test_vm_conn_id = "test_azure_batch_vm_default"
        self.test_cloud_conn_id = "test_azure_batch_cloud_default"
        self.test_account_name = "test_account_name"
        self.test_account_key = "test_account_key"
        self.test_account_url = "http://test-endpoint:29000"
        self.test_vm_size = "test-vm-size"
        self.test_vm_publisher = "test.vm.publisher"
        self.test_vm_offer = "test.vm.offer"
        self.test_vm_sku = "test-sku"
        self.test_cloud_os_family = "test-family"
        self.test_cloud_os_version = "test-version"
        self.test_node_agent_sku = "test-node-agent-sku"

        # connect with vm configuration
        db.merge_conn(
            Connection(conn_id=self.test_vm_conn_id,
                       conn_type="azure_batch",
                       extra=json.dumps({
                           "AZ_BATCH_ACCOUNT_NAME": self.test_account_name,
                           "AZ_BATCH_ACCOUNT_KEY": self.test_account_key,
                           "AZ_BATCH_ACCOUNT_URL": self.test_account_url,
                           "AZ_BATCH_VM_PUBLISHER": self.test_vm_publisher,
                           "AZ_BATCH_VM_OFFER": self.test_vm_offer,
                           "AZ_BATCH_VM_SKU": self.test_vm_sku,
                           "AZ_BATCH_NODE_AGENT_SKU_ID": self.test_node_agent_sku
                       }))
        )
        # connect with cloud service
        db.merge_conn(
            Connection(conn_id=self.test_cloud_conn_id,
                       conn_type="azure_batch",
                       extra=json.dumps({
                           "AZ_BATCH_ACCOUNT_NAME": self.test_account_name,
                           "AZ_BATCH_ACCOUNT_KEY": self.test_account_key,
                           "AZ_BATCH_ACCOUNT_URL": self.test_account_url,
                           "AZ_BATCH_CLOUD_OS_FAMILY": self.test_cloud_os_family,
                           "AZ_BATCH_CLOUD_OS_VERSION": self.test_cloud_os_version,
                           "AZ_BATCH_NODE_AGENT_SKU_ID": self.test_node_agent_sku
                       }))
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_batch.AzureBatchHook")
    def test_executes_with_only_required_items(self, mock_hook):
        operator = AzureBatchOperator(
            task_id=TASK_ID,
            batch_pool_id=self.batch_pool_id,
            batch_pool_vm_size=self.vm_size,
            batch_job_id=self.batch_job_id,
            batch_task_id=self.batch_task_id,
            batch_task_command_line="echo hello",
            azure_batch_conn_id=self.test_vm_conn_id
        )
        operator.execute(None)
        mock_hook.return_value.configure_pool.assert_called_once_with(pool_id=self.batch_pool_id,
                                                                      vm_size=self.vm_size)
