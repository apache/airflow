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

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.glue_catalog import (
    GlueCatalogCreateDatabaseOperator,
    GlueCatalogCreateTableOperator,
    GlueCatalogDeleteDatabaseOperator,
    GlueCatalogDeleteTableOperator,
)

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

DB_NAME = "test_database"


class TestGlueCatalogCreateDatabaseOperator:
    def test_execute(self):
        op = GlueCatalogCreateDatabaseOperator(
            task_id="create_db",
            database_name=DB_NAME,
        )
        mock_conn = MagicMock()
        op.hook.conn = mock_conn

        result = op.execute({})

        mock_conn.create_database.assert_called_once_with(DatabaseInput={"Name": DB_NAME})
        assert result == DB_NAME

    def test_execute_with_all_params(self):
        op = GlueCatalogCreateDatabaseOperator(
            task_id="create_db",
            database_name=DB_NAME,
            description="Test database",
            location_uri="s3://my-bucket/db/",
            parameters={"key": "value"},
            catalog_id="123456789012",
            tags={"env": "test"},
        )
        mock_conn = MagicMock()
        op.hook.conn = mock_conn

        result = op.execute({})

        mock_conn.create_database.assert_called_once_with(
            DatabaseInput={
                "Name": DB_NAME,
                "Description": "Test database",
                "LocationUri": "s3://my-bucket/db/",
                "Parameters": {"key": "value"},
            },
            CatalogId="123456789012",
            Tags={"env": "test"},
        )
        assert result == DB_NAME

    def test_execute_skip_existing(self):
        op = GlueCatalogCreateDatabaseOperator(
            task_id="create_db",
            database_name=DB_NAME,
            if_exists="skip",
        )
        mock_conn = MagicMock()
        mock_conn.create_database.side_effect = ClientError(
            {"Error": {"Code": "AlreadyExistsException", "Message": "Already exists"}},
            "CreateDatabase",
        )
        op.hook.conn = mock_conn

        result = op.execute({})

        assert result == DB_NAME

    def test_template_fields(self):
        op = GlueCatalogCreateDatabaseOperator(
            task_id="test",
            database_name=DB_NAME,
        )
        validate_template_fields(op)


class TestGlueCatalogDeleteDatabaseOperator:
    def test_execute(self):
        op = GlueCatalogDeleteDatabaseOperator(
            task_id="delete_db",
            database_name=DB_NAME,
        )
        mock_conn = MagicMock()
        op.hook.conn = mock_conn

        op.execute({})

        mock_conn.delete_database.assert_called_once_with(Name=DB_NAME)

    def test_execute_with_catalog_id(self):
        op = GlueCatalogDeleteDatabaseOperator(
            task_id="delete_db",
            database_name=DB_NAME,
            catalog_id="123456789012",
        )
        mock_conn = MagicMock()
        op.hook.conn = mock_conn

        op.execute({})

        mock_conn.delete_database.assert_called_once_with(
            Name=DB_NAME,
            CatalogId="123456789012",
        )

    def test_template_fields(self):
        op = GlueCatalogDeleteDatabaseOperator(
            task_id="test",
            database_name=DB_NAME,
        )
        validate_template_fields(op)


TABLE_NAME = "test_table"
TABLE_INPUT = {
    "StorageDescriptor": {
        "Columns": [{"Name": "id", "Type": "int"}, {"Name": "name", "Type": "string"}],
        "Location": "s3://bucket/path/",
        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
    },
    "TableType": "EXTERNAL_TABLE",
}


class TestGlueCatalogCreateTableOperator:
    def setup_method(self):
        self.operator = GlueCatalogCreateTableOperator(
            task_id="create_table",
            database_name=DB_NAME,
            table_name=TABLE_NAME,
            table_input=TABLE_INPUT,
        )

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client

        result = self.operator.execute({})

        call_kwargs = mock_client.create_table.call_args[1]
        assert call_kwargs["DatabaseName"] == DB_NAME
        assert call_kwargs["TableInput"]["Name"] == TABLE_NAME
        assert result == TABLE_NAME

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_skip_existing(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_client.create_table.side_effect = ClientError(
            {"Error": {"Code": "AlreadyExistsException", "Message": "Already exists"}},
            "CreateTable",
        )
        mock_conn.return_value = mock_client

        result = self.operator.execute({})
        assert result == TABLE_NAME

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute_fail_on_conflict(self, mock_conn):
        op = GlueCatalogCreateTableOperator(
            task_id="create_table",
            database_name=DB_NAME,
            table_name=TABLE_NAME,
            table_input=TABLE_INPUT,
            if_exists="fail",
        )
        mock_client = mock.MagicMock()
        mock_client.create_table.side_effect = ClientError(
            {"Error": {"Code": "AlreadyExistsException", "Message": "Already exists"}},
            "CreateTable",
        )
        mock_conn.return_value = mock_client

        with pytest.raises(ClientError):
            op.execute({})

    def test_template_fields(self):
        validate_template_fields(self.operator)


class TestGlueCatalogDeleteTableOperator:
    def setup_method(self):
        self.operator = GlueCatalogDeleteTableOperator(
            task_id="delete_table",
            database_name=DB_NAME,
            table_name=TABLE_NAME,
        )

    @mock.patch.object(AwsBaseHook, "conn", new_callable=mock.PropertyMock)
    def test_execute(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_conn.return_value = mock_client

        self.operator.execute({})

        mock_client.delete_table.assert_called_once_with(DatabaseName=DB_NAME, Name=TABLE_NAME)

    def test_template_fields(self):
        validate_template_fields(self.operator)
