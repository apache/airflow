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

from unittest import mock

from airflow.providers.google.cloud.operators.datastore import (
    CloudDatastoreAllocateIdsOperator,
    CloudDatastoreBeginTransactionOperator,
    CloudDatastoreCommitOperator,
    CloudDatastoreDeleteOperationOperator,
    CloudDatastoreExportEntitiesOperator,
    CloudDatastoreGetOperationOperator,
    CloudDatastoreImportEntitiesOperator,
    CloudDatastoreRollbackOperator,
    CloudDatastoreRunQueryOperator,
)

HOOK_PATH = "airflow.providers.google.cloud.operators.datastore.DatastoreHook"
PROJECT_ID = "test-project"
CONN_ID = "test-gcp-conn-id"
BODY = {"key", "value"}
TRANSACTION = "transaction-name"
BUCKET = "gs://test-bucket"
FILE = "filename"
OPERATION_ID = "1234"


class TestCloudDatastoreExportEntitiesOperator:
    @mock.patch(HOOK_PATH)
    def test_execute(self, mock_hook):
        mock_hook.return_value.export_to_storage_bucket.return_value = {"name": OPERATION_ID}
        mock_hook.return_value.poll_operation_until_done.return_value = {
            "metadata": {"common": {"state": "SUCCESSFUL"}}
        }

        op = CloudDatastoreExportEntitiesOperator(
            task_id="test_task", datastore_conn_id=CONN_ID, project_id=PROJECT_ID, bucket=BUCKET,
        )
        op.execute({})

        mock_hook.assert_called_once_with(CONN_ID, None, impersonation_chain=None)
        mock_hook.return_value.export_to_storage_bucket.assert_called_once_with(
            project_id=PROJECT_ID, bucket=BUCKET, entity_filter=None, labels=None, namespace=None,
        )

        mock_hook.return_value.poll_operation_until_done.assert_called_once_with(OPERATION_ID, 10)


class TestCloudDatastoreImportEntitiesOperator:
    @mock.patch(HOOK_PATH)
    def test_execute(self, mock_hook):
        mock_hook.return_value.import_from_storage_bucket.return_value = {"name": OPERATION_ID}
        mock_hook.return_value.poll_operation_until_done.return_value = {
            "metadata": {"common": {"state": "SUCCESSFUL"}}
        }

        op = CloudDatastoreImportEntitiesOperator(
            task_id="test_task", datastore_conn_id=CONN_ID, project_id=PROJECT_ID, bucket=BUCKET, file=FILE,
        )
        op.execute({})

        mock_hook.assert_called_once_with(CONN_ID, None, impersonation_chain=None)
        mock_hook.return_value.import_from_storage_bucket.assert_called_once_with(
            project_id=PROJECT_ID, bucket=BUCKET, file=FILE, entity_filter=None, labels=None, namespace=None,
        )

        mock_hook.return_value.export_to_storage_bucketassert_called_once_with(OPERATION_ID, 10)


class TestCloudDatastoreAllocateIds:
    @mock.patch(HOOK_PATH)
    def test_execute(self, mock_hook):
        partial_keys = [1, 2, 3]
        op = CloudDatastoreAllocateIdsOperator(
            task_id="test_task", gcp_conn_id=CONN_ID, project_id=PROJECT_ID, partial_keys=partial_keys,
        )
        op.execute({})

        mock_hook.assert_called_once_with(gcp_conn_id=CONN_ID, impersonation_chain=None)
        mock_hook.return_value.allocate_ids.assert_called_once_with(
            project_id=PROJECT_ID, partial_keys=partial_keys
        )


class TestCloudDatastoreBeginTransaction:
    @mock.patch(HOOK_PATH)
    def test_execute(self, mock_hook):
        op = CloudDatastoreBeginTransactionOperator(
            task_id="test_task", gcp_conn_id=CONN_ID, project_id=PROJECT_ID, transaction_options=BODY,
        )
        op.execute({})

        mock_hook.assert_called_once_with(gcp_conn_id=CONN_ID, impersonation_chain=None)
        mock_hook.return_value.begin_transaction.assert_called_once_with(
            project_id=PROJECT_ID, transaction_options=BODY
        )


class TestCloudDatastoreCommit:
    @mock.patch(HOOK_PATH)
    def test_execute(self, mock_hook):
        op = CloudDatastoreCommitOperator(
            task_id="test_task", gcp_conn_id=CONN_ID, project_id=PROJECT_ID, body=BODY
        )
        op.execute({})

        mock_hook.assert_called_once_with(gcp_conn_id=CONN_ID, impersonation_chain=None)
        mock_hook.return_value.commit.assert_called_once_with(project_id=PROJECT_ID, body=BODY)


class TestCloudDatastoreDeleteOperation:
    @mock.patch(HOOK_PATH)
    def test_execute(self, mock_hook):
        op = CloudDatastoreDeleteOperationOperator(task_id="test_task", gcp_conn_id=CONN_ID, name=TRANSACTION)
        op.execute({})

        mock_hook.assert_called_once_with(gcp_conn_id=CONN_ID, impersonation_chain=None)
        mock_hook.return_value.delete_operation.assert_called_once_with(name=TRANSACTION)


class TestCloudDatastoreGetOperation:
    @mock.patch(HOOK_PATH)
    def test_execute(self, mock_hook):
        op = CloudDatastoreGetOperationOperator(task_id="test_task", gcp_conn_id=CONN_ID, name=TRANSACTION)
        op.execute({})

        mock_hook.assert_called_once_with(gcp_conn_id=CONN_ID, impersonation_chain=None)
        mock_hook.return_value.get_operation.assert_called_once_with(name=TRANSACTION)


class TestCloudDatastoreRollback:
    @mock.patch(HOOK_PATH)
    def test_execute(self, mock_hook):
        op = CloudDatastoreRollbackOperator(
            task_id="test_task", gcp_conn_id=CONN_ID, project_id=PROJECT_ID, transaction=TRANSACTION,
        )
        op.execute({})

        mock_hook.assert_called_once_with(gcp_conn_id=CONN_ID, impersonation_chain=None)
        mock_hook.return_value.rollback.assert_called_once_with(
            project_id=PROJECT_ID, transaction=TRANSACTION
        )


class TestCloudDatastoreRunQuery:
    @mock.patch(HOOK_PATH)
    def test_execute(self, mock_hook):
        op = CloudDatastoreRunQueryOperator(
            task_id="test_task", gcp_conn_id=CONN_ID, project_id=PROJECT_ID, body=BODY
        )
        op.execute({})

        mock_hook.assert_called_once_with(gcp_conn_id=CONN_ID, impersonation_chain=None)
        mock_hook.return_value.run_query.assert_called_once_with(project_id=PROJECT_ID, body=BODY)
