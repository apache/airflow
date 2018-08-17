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
from __future__ import unicode_literals

import unittest
import mock
from builtins import str
from airflow.contrib.operators.cassandra_to_gcs import \
    CassandraToGoogleCloudStorageOperator


class CassandraToGCSTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.gcs_to_s3.GoogleCloudStorageHook.upload')
    @mock.patch('airflow.contrib.hooks.cassandra_hook.CassandraHook.get_conn')
    def test_execute(self, upload, get_conn):
        operator = CassandraToGoogleCloudStorageOperator(
            task_id='test-cas-to-gcs',
            cql='select * from keyspace1.table1',
            bucket='test-bucket',
            filename='data.json',
            schema_filename='schema.json')

        operator.execute(None)

        self.assertTrue(get_conn.called_once())
        self.assertTrue(upload.called_once())

    def test_convert_value(self):
        op = CassandraToGoogleCloudStorageOperator
        self.assertEquals(op.convert_value('None', None), None)
        self.assertEquals(op.convert_value('int', 1), 1)
        self.assertEquals(op.convert_value('float', 1.0), 1.0)
        self.assertEquals(op.convert_value('str', "text"), "text")
        self.assertEquals(op.convert_value('bool', True), True)
        self.assertEquals(op.convert_value('dict', {"a": "b"}), {"a": "b"})

        from datetime import datetime
        now = datetime.now()
        self.assertEquals(op.convert_value('datetime', now), str(now))

        from cassandra.util import Date
        date_str = '2018-01-01'
        date = Date(date_str)
        self.assertEquals(op.convert_value('date', date), str(date_str))

        import uuid
        from base64 import b64encode
        test_uuid = uuid.uuid4()
        encoded_uuid = b64encode(test_uuid.bytes).decode('ascii')
        self.assertEquals(op.convert_value('uuid', test_uuid), encoded_uuid)

        b = b'abc'
        encoded_b = b64encode(b).decode('ascii')
        self.assertEquals(op.convert_value('binary', b), encoded_b)

        from decimal import Decimal
        d = Decimal(1.0)
        self.assertEquals(op.convert_value('decimal', d), float(d))

        from cassandra.util import Time
        time = Time(0)
        self.assertEquals(op.convert_value('time', time), '00:00:00')

        date_str_lst = ['2018-01-01', '2018-01-02', '2018-01-03']
        date_lst = [Date(d) for d in date_str_lst]
        self.assertEquals(op.convert_value('list', date_lst), date_str_lst)

        date_tpl = tuple(date_lst)
        self.assertEquals(op.convert_value('tuple', date_tpl),
                          {'field_0': '2018-01-01',
                           'field_1': '2018-01-02',
                           'field_2': '2018-01-03', })


if __name__ == '__main__':
    unittest.main()
