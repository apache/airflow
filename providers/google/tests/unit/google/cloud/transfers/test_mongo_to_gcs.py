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
from __future__ import annotations

import datetime
from decimal import Decimal
from unittest import mock

import pytest
from bson import ObjectId
from bson.decimal128 import Decimal128

from airflow.providers.google.cloud.transfers.mongo_to_gcs import (
    MongoToGCSOperator,
    _MongoCursorAdapter,
)

TASK_ID = "test-mongo-to-gcs"
MONGO_CONN_ID = "mongo_test"
MONGO_DB = "test_db"
MONGO_COLLECTION = "test_collection"
BUCKET = "gs://test"
JSON_FILENAME = "test_{}.ndjson"


class TestMongoCursorAdapter:
    def test_empty_cursor(self):
        adapter = _MongoCursorAdapter(iter([]))
        assert adapter.description == []
        assert list(adapter) == []

    def test_description_derived_from_first_doc(self):
        docs = [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]
        adapter = _MongoCursorAdapter(iter(docs))

        names = [d[0] for d in adapter.description]
        types = [d[1] for d in adapter.description]
        assert names == ["name", "age"]
        assert types == [str, int]

    def test_iteration_returns_tuples_in_description_order(self):
        docs = [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]
        adapter = _MongoCursorAdapter(iter(docs))

        rows = list(adapter)
        assert rows == [("alice", 30), ("bob", 25)]

    def test_missing_field_filled_with_none(self):
        docs = [{"name": "alice", "age": 30}, {"name": "bob"}]
        adapter = _MongoCursorAdapter(iter(docs))

        rows = list(adapter)
        assert rows == [("alice", 30), ("bob", None)]


class TestMongoToGCSOperator:
    def test_init(self):
        op = MongoToGCSOperator(
            task_id=TASK_ID,
            mongo_conn_id=MONGO_CONN_ID,
            mongo_db=MONGO_DB,
            mongo_collection=MONGO_COLLECTION,
            bucket=BUCKET,
            filename=JSON_FILENAME,
        )
        assert op.task_id == TASK_ID
        assert op.mongo_conn_id == MONGO_CONN_ID
        assert op.mongo_db == MONGO_DB
        assert op.mongo_collection == MONGO_COLLECTION
        assert op.mongo_query == {}
        assert op.mongo_projection is None
        assert op.allow_disk_use is True
        assert op.bucket == BUCKET
        assert op.filename == JSON_FILENAME

    def test_field_to_bigquery_known_type(self):
        op = MongoToGCSOperator(
            task_id=TASK_ID,
            mongo_db=MONGO_DB,
            mongo_collection=MONGO_COLLECTION,
            bucket=BUCKET,
            filename=JSON_FILENAME,
        )
        assert op.field_to_bigquery(("col", int, None, None, None, None, True)) == {
            "name": "col",
            "type": "INTEGER",
            "mode": "NULLABLE",
        }

    def test_field_to_bigquery_unknown_type_defaults_to_string(self):
        op = MongoToGCSOperator(
            task_id=TASK_ID,
            mongo_db=MONGO_DB,
            mongo_collection=MONGO_COLLECTION,
            bucket=BUCKET,
            filename=JSON_FILENAME,
        )

        class _Custom:
            pass

        assert op.field_to_bigquery(("col", _Custom, None, None, None, None, True))["type"] == "STRING"

    @pytest.mark.parametrize(
        ("value", "schema_type", "expected"),
        [
            (None, None, None),
            ("text", None, "text"),
            (42, None, 42),
            (3.14, None, 3.14),
            (True, None, True),
            (Decimal("1.5"), None, 1.5),
            (datetime.datetime(2023, 1, 2, 3, 4, 5), None, "2023-01-02 03:04:05"),
            (datetime.date(2023, 1, 2), "DATE", "2023-01-02"),
            (datetime.date(2023, 1, 2), None, "2023-01-02 00:00:00"),
            ([1, 2, 3], None, "[1, 2, 3]"),
            ({"a": 1}, None, '{"a": 1}'),
            ((1, 2), None, "[1, 2]"),
        ],
    )
    def test_convert_type_basic(self, value, schema_type, expected):
        op = MongoToGCSOperator(
            task_id=TASK_ID,
            mongo_db=MONGO_DB,
            mongo_collection=MONGO_COLLECTION,
            bucket=BUCKET,
            filename=JSON_FILENAME,
        )
        assert op.convert_type(value, schema_type) == expected

    def test_convert_type_object_id(self):
        op = MongoToGCSOperator(
            task_id=TASK_ID,
            mongo_db=MONGO_DB,
            mongo_collection=MONGO_COLLECTION,
            bucket=BUCKET,
            filename=JSON_FILENAME,
        )
        oid = ObjectId("507f1f77bcf86cd799439011")
        assert op.convert_type(oid, None) == "507f1f77bcf86cd799439011"

    def test_convert_type_decimal128(self):
        op = MongoToGCSOperator(
            task_id=TASK_ID,
            mongo_db=MONGO_DB,
            mongo_collection=MONGO_COLLECTION,
            bucket=BUCKET,
            filename=JSON_FILENAME,
        )
        assert op.convert_type(Decimal128("3.14"), None) == 3.14

    def test_convert_type_bytes_default_base64(self):
        op = MongoToGCSOperator(
            task_id=TASK_ID,
            mongo_db=MONGO_DB,
            mongo_collection=MONGO_COLLECTION,
            bucket=BUCKET,
            filename=JSON_FILENAME,
        )
        assert op.convert_type(b"hello", None) == "aGVsbG8="

    def test_convert_type_bytes_as_integer(self):
        op = MongoToGCSOperator(
            task_id=TASK_ID,
            mongo_db=MONGO_DB,
            mongo_collection=MONGO_COLLECTION,
            bucket=BUCKET,
            filename=JSON_FILENAME,
        )
        assert op.convert_type(b"\x00\x01", "INTEGER") == 1

    @mock.patch("airflow.providers.google.cloud.transfers.mongo_to_gcs.MongoHook", autospec=True)
    def test_query_with_find_filter(self, mock_hook_class):
        mock_collection = mock.MagicMock()
        mock_hook_class.return_value.get_conn.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter([{"_id": ObjectId(), "name": "alice"}])

        op = MongoToGCSOperator(
            task_id=TASK_ID,
            mongo_conn_id=MONGO_CONN_ID,
            mongo_db=MONGO_DB,
            mongo_collection=MONGO_COLLECTION,
            mongo_query={"name": "alice"},
            mongo_projection={"name": 1},
            bucket=BUCKET,
            filename=JSON_FILENAME,
        )

        cursor = op.query()
        mock_collection.find.assert_called_once_with({"name": "alice"}, projection={"name": 1})
        mock_collection.aggregate.assert_not_called()
        assert isinstance(cursor, _MongoCursorAdapter)

    @mock.patch("airflow.providers.google.cloud.transfers.mongo_to_gcs.MongoHook", autospec=True)
    def test_query_with_aggregation_pipeline(self, mock_hook_class):
        mock_collection = mock.MagicMock()
        mock_hook_class.return_value.get_conn.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
        mock_collection.aggregate.return_value = iter([{"_id": "alice", "count": 1}])

        pipeline = [{"$match": {"active": True}}, {"$group": {"_id": "$name", "count": {"$sum": 1}}}]
        op = MongoToGCSOperator(
            task_id=TASK_ID,
            mongo_conn_id=MONGO_CONN_ID,
            mongo_db=MONGO_DB,
            mongo_collection=MONGO_COLLECTION,
            mongo_query=pipeline,
            allow_disk_use=False,
            bucket=BUCKET,
            filename=JSON_FILENAME,
        )

        cursor = op.query()
        mock_collection.aggregate.assert_called_once_with(pipeline, allowDiskUse=False)
        mock_collection.find.assert_not_called()
        assert isinstance(cursor, _MongoCursorAdapter)

    @mock.patch("airflow.providers.google.cloud.transfers.mongo_to_gcs.MongoHook", autospec=True)
    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook", autospec=True)
    def test_execute_uploads_to_gcs(self, gcs_hook_mock_class, mongo_hook_mock_class):
        mock_collection = mock.MagicMock()
        mongo_hook_mock_class.return_value.get_conn.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter(
            [
                {"name": "alice", "age": 30},
                {"name": "bob", "age": 25},
            ]
        )

        gcs_hook_mock = gcs_hook_mock_class.return_value
        upload_called = False

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None, gzip=False, metadata=None):
            nonlocal upload_called
            upload_called = True
            assert bucket == BUCKET
            assert obj == JSON_FILENAME.format(0)
            assert mime_type == "application/json"
            with open(tmp_filename, "rb") as fh:
                lines = fh.read().splitlines()
            assert lines == [
                b'{"age": 30, "name": "alice"}',
                b'{"age": 25, "name": "bob"}',
            ]

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MongoToGCSOperator(
            task_id=TASK_ID,
            mongo_conn_id=MONGO_CONN_ID,
            mongo_db=MONGO_DB,
            mongo_collection=MONGO_COLLECTION,
            bucket=BUCKET,
            filename=JSON_FILENAME,
        )
        op.execute(None)

        assert upload_called, "Expected GCS upload to be called"
        mongo_hook_mock_class.assert_called_once_with(mongo_conn_id=MONGO_CONN_ID)
