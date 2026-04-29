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

from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.operators.glue_catalog import (
    GlueCatalogCreateDatabaseOperator,
    GlueCatalogDeleteDatabaseOperator,
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
