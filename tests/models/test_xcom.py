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
from datetime import datetime

from airflow.models import XCom


class TestXCom(unittest.TestCase):
    def test_serialize_value_success(self):
        # given
        value = {
            "execution_date": datetime(1995, 2, 3, 18, 0, 0, 0),
            "string": "string",
            "array": ["array", "array"],
            "number": 42,
            "object": {
                "array": {1, 2, 3}
            }
        }

        # when
        serialized = XCom.serialize_value(value)

        # then
        self.assertEqual("", serialized)
