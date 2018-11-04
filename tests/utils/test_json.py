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

from datetime import datetime, date
import json
import unittest

import numpy as np

from airflow.utils import json as utils_json


class TestJsonSer(unittest.TestCase):

    def test_json_ser_datetime(self):
        obj = datetime.strptime('2017-05-21 00:00:00', '%Y-%m-%d %H:%M:%S')
        self.assertEqual(
            json.dumps(obj, default=utils_json.json_ser),
            '"2017-05-21T00:00:00"',
        )

    def test_json_ser_date(self):
        self.assertEqual(
            json.dumps(date(2017, 5, 21), default=utils_json.json_ser),
            '"2017-05-21"',
        )


class TestAirflowJsonEncoder(unittest.TestCase):

    def test_encode_datetime(self):
        obj = datetime.strptime('2017-05-21 00:00:00', '%Y-%m-%d %H:%M:%S')
        self.assertEqual(
            json.dumps(obj, cls=utils_json.AirflowJsonEncoder),
            '"2017-05-21T00:00:00Z"'
        )

    def test_encode_date(self):
        self.assertEqual(
            json.dumps(date(2017, 5, 21), cls=utils_json.AirflowJsonEncoder),
            '"2017-05-21"'
        )

    def test_encode_numpy_int(self):
        self.assertEqual(
            json.dumps(np.int32(5), cls=utils_json.AirflowJsonEncoder),
            '5'
        )

    def test_encode_numpy_bool(self):
        self.assertEqual(
            json.dumps(np.bool_(True), cls=utils_json.AirflowJsonEncoder),
            'true'
        )

    def test_encode_numpy_float(self):
        self.assertEqual(
            json.dumps(np.float16(3.76953125), cls=utils_json.AirflowJsonEncoder),
            '3.76953125'
        )

    def test_encode_raises(self):
        self.assertRaisesRegexp(TypeError,
                                "^%s is not JSON serializable$" % Exception,
                                json.dumps,
                                Exception,
                                cls=utils_json.AirflowJsonEncoder)


if __name__ == '__main__':
    unittest.main()
