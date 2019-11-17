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

import unittest
from unittest import mock

from mock import call

from airflow.operators.cassandra_to_gcs import CassandraToGoogleCloudStorageOperator

TMP_FILE_NAME = "temp-file"


class TestCassandraToGCS(unittest.TestCase):
    @mock.patch("airflow.operators.cassandra_to_gcs.NamedTemporaryFile")
    @mock.patch(
        "airflow.operators.cassandra_to_gcs.GoogleCloudStorageHook.upload"
    )
    @mock.patch("airflow.operators.cassandra_to_gcs.CassandraHook")
    def test_execute(self, mock_hook, mock_upload, mock_tempfile):
        test_bucket = "test-bucket"
        schema = "schema.json"
        filename = "data.json"
        gzip = True
        mock_tempfile.return_value.name = TMP_FILE_NAME

        operator = CassandraToGoogleCloudStorageOperator(
            task_id="test-cas-to-gcs",
            cql="select * from keyspace1.table1",
            bucket=test_bucket,
            filename=filename,
            schema_filename=schema,
            gzip=gzip,
        )
        operator.execute(None)
        mock_hook.return_value.get_conn.assert_called_once_with()

        call_schema = call(test_bucket, schema, TMP_FILE_NAME, "application/json", gzip)
        call_data = call(test_bucket, filename, TMP_FILE_NAME, "application/json", gzip)
        mock_upload.assert_has_calls([call_schema, call_data], any_order=True)

    def test_convert_value(self):
        op = CassandraToGoogleCloudStorageOperator
        self.assertEqual(op.convert_value("None", None), None)
        self.assertEqual(op.convert_value("int", 1), 1)
        self.assertEqual(op.convert_value("float", 1.0), 1.0)
        self.assertEqual(op.convert_value("str", "text"), "text")
        self.assertEqual(op.convert_value("bool", True), True)
        self.assertEqual(op.convert_value("dict", {"a": "b"}), {"a": "b"})

        from datetime import datetime

        now = datetime.now()
        self.assertEqual(op.convert_value("datetime", now), str(now))

        from cassandra.util import Date

        date_str = "2018-01-01"
        date = Date(date_str)
        self.assertEqual(op.convert_value("date", date), str(date_str))

        import uuid
        from base64 import b64encode

        test_uuid = uuid.uuid4()
        encoded_uuid = b64encode(test_uuid.bytes).decode("ascii")
        self.assertEqual(op.convert_value("uuid", test_uuid), encoded_uuid)

        b = b"abc"
        encoded_b = b64encode(b).decode("ascii")
        self.assertEqual(op.convert_value("binary", b), encoded_b)

        from decimal import Decimal

        d = Decimal(1.0)
        self.assertEqual(op.convert_value("decimal", d), float(d))

        from cassandra.util import Time

        time = Time(0)
        self.assertEqual(op.convert_value("time", time), "00:00:00")

        date_str_lst = ["2018-01-01", "2018-01-02", "2018-01-03"]
        date_lst = [Date(d) for d in date_str_lst]
        self.assertEqual(op.convert_value("list", date_lst), date_str_lst)

        date_tpl = tuple(date_lst)
        self.assertEqual(
            op.convert_value("tuple", date_tpl),
            {"field_0": "2018-01-01", "field_1": "2018-01-02", "field_2": "2018-01-03"},
        )
