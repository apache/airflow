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

from unittest import mock
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.opensearch_serverless import OpenSearchServerlessHook
from airflow.providers.amazon.aws.operators.opensearch_serverless import (
    OpenSearchServerlessCreateCollectionOperator,
)

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

COLLECTION_NAME = "test-collection"
COLLECTION_ID = "abc123def456"


class TestOpenSearchServerlessCreateCollectionOperator:
    def setup_method(self):
        self.operator = OpenSearchServerlessCreateCollectionOperator(
            task_id="create_collection",
            collection_name=COLLECTION_NAME,
            collection_type="VECTORSEARCH",
        )

    @mock.patch.object(OpenSearchServerlessHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = MagicMock()
        mock_client.create_collection.return_value = {
            "createCollectionDetail": {"id": COLLECTION_ID, "name": COLLECTION_NAME}
        }
        mock_conn.return_value = mock_client

        result = self.operator.execute({})

        mock_client.create_collection.assert_called_once_with(name=COLLECTION_NAME, type="VECTORSEARCH")
        assert result == COLLECTION_ID

    @mock.patch.object(OpenSearchServerlessHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_skip_existing(self, mock_conn):
        mock_client = MagicMock()
        mock_client.create_collection.side_effect = ClientError(
            {"Error": {"Code": "ConflictException", "Message": "exists"}}, "CreateCollection"
        )
        mock_client.batch_get_collection.return_value = {
            "collectionDetails": [{"id": COLLECTION_ID, "name": COLLECTION_NAME}]
        }
        mock_conn.return_value = mock_client

        result = self.operator.execute({})

        mock_client.create_collection.assert_called_once_with(name=COLLECTION_NAME, type="VECTORSEARCH")
        mock_client.batch_get_collection.assert_called_once_with(names=[COLLECTION_NAME])
        assert result == COLLECTION_ID

    @mock.patch.object(OpenSearchServerlessHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_fail_on_conflict(self, mock_conn):
        op = OpenSearchServerlessCreateCollectionOperator(
            task_id="create_collection",
            collection_name=COLLECTION_NAME,
            if_exists="fail",
        )
        mock_client = MagicMock()
        mock_client.create_collection.side_effect = ClientError(
            {"Error": {"Code": "ConflictException", "Message": "exists"}}, "CreateCollection"
        )
        mock_conn.return_value = mock_client

        with pytest.raises(ClientError, match="ConflictException"):
            op.execute({})

    def test_template_fields(self):
        validate_template_fields(self.operator)
