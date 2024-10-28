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

from unittest import mock
from unittest.mock import call

import pytest
from _pytest.outcomes import importorskip

cassandra = importorskip("cassandra")

TMP_FILE_NAME = "temp-file"
TEST_BUCKET = "test-bucket"
SCHEMA = "schema.json"
FILENAME = "data.json"
CQL = "select * from keyspace1.table1"
TASK_ID = "test-cas-to-gcs"


class TestCassandraToGCS:
    @pytest.mark.db_test
    def test_execute(self):
        test_bucket = TEST_BUCKET
        schema = SCHEMA
        filename = FILENAME
        gzip = True
        query_timeout = 20
        try:
            from airflow.providers.google.cloud.transfers.cassandra_to_gcs import (
                CassandraToGCSOperator,
            )
        except cassandra.DependencyException:
            pytest.skip(
                "cassandra-driver not installed with libev support. Skipping test."
            )

        with (
            mock.patch(
                "airflow.providers.google.cloud.transfers.cassandra_to_gcs.NamedTemporaryFile"
            ) as mock_tempfile,
            mock.patch(
                "airflow.providers.google.cloud.transfers.cassandra_to_gcs.GCSHook.upload"
            ) as mock_upload,
            mock.patch(
                "airflow.providers.google.cloud.transfers.cassandra_to_gcs.CassandraHook"
            ) as mock_hook,
        ):
            mock_tempfile.return_value.name = TMP_FILE_NAME
            operator = CassandraToGCSOperator(
                task_id=TASK_ID,
                cql=CQL,
                bucket=test_bucket,
                filename=filename,
                schema_filename=schema,
                gzip=gzip,
                query_timeout=query_timeout,
            )
            operator.execute(None)
            mock_hook.return_value.get_conn.assert_called_once_with()
            mock_hook.return_value.get_conn.return_value.execute.assert_called_once_with(
                "select * from keyspace1.table1",
                timeout=20,
            )

            call_schema = call(
                bucket_name=test_bucket,
                object_name=schema,
                filename=TMP_FILE_NAME,
                mime_type="application/json",
                gzip=gzip,
            )
            call_data = call(
                bucket_name=test_bucket,
                object_name=filename,
                filename=TMP_FILE_NAME,
                mime_type="application/json",
                gzip=gzip,
            )
            mock_upload.assert_has_calls([call_schema, call_data], any_order=True)

    def test_convert_value(self):
        try:
            from airflow.providers.google.cloud.transfers.cassandra_to_gcs import (
                CassandraToGCSOperator,
            )
        except cassandra.DependencyException:
            pytest.skip(
                "cassandra-driver not installed with libev support. Skipping test."
            )

        op = CassandraToGCSOperator(
            task_id=TASK_ID, bucket=TEST_BUCKET, cql=CQL, filename=FILENAME
        )
        unencoded_uuid_op = CassandraToGCSOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            cql=CQL,
            filename=FILENAME,
            encode_uuid=False,
        )
        assert op.convert_value(None) is None
        assert op.convert_value(1) == 1
        assert op.convert_value(1.0) == 1.0
        assert op.convert_value("text") == "text"
        assert op.convert_value(True) is True
        assert op.convert_value({"a": "b"}) == {"a": "b"}

        from datetime import datetime

        now = datetime.now()
        assert op.convert_value(now) == str(now)

        from cassandra.util import Date

        date_str = "2018-01-01"
        date = Date(date_str)
        assert op.convert_value(date) == str(date_str)

        import uuid
        from base64 import b64encode

        test_uuid = uuid.uuid4()
        encoded_uuid = b64encode(test_uuid.bytes).decode("ascii")
        assert op.convert_value(test_uuid) == encoded_uuid
        unencoded_uuid = str(test_uuid)
        assert unencoded_uuid_op.convert_value(test_uuid) == unencoded_uuid

        byte_str = b"abc"
        encoded_b = b64encode(byte_str).decode("ascii")
        assert op.convert_value(byte_str) == encoded_b

        from decimal import Decimal

        decimal = Decimal(1.0)
        assert op.convert_value(decimal) == float(decimal)

        from cassandra.util import Time

        time = Time(0)
        assert op.convert_value(time) == "00:00:00"

        date_str_lst = ["2018-01-01", "2018-01-02", "2018-01-03"]
        date_lst = [Date(d) for d in date_str_lst]
        assert op.convert_value(date_lst) == date_str_lst

        date_tpl = tuple(date_lst)
        assert op.convert_value(date_tpl) == {
            "field_0": "2018-01-01",
            "field_1": "2018-01-02",
            "field_2": "2018-01-03",
        }
