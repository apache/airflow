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

import pytest

from airflow.models.param import Param


class TestDagRun(unittest.TestCase):
    def test_param_without_schema(self):
        p = Param('test')
        assert p() == 'test'

        p.default = 10
        assert p() == 10

    def test_null_param(self):
        p = Param()
        assert p() is None

        p = Param(type="null")
        assert p() is None

    def test_string_param(self):
        p = Param('test', type='string')
        assert p() == 'test'

        p = Param('test')
        assert p() == 'test'

        p = Param('10.0.0.0', type='string', format='ipv4')
        assert p() == '10.0.0.0'

        with pytest.raises(ValueError):
            p = Param(type='string')
            p()

    def test_int_param(self):
        p = Param(5)
        assert p() == 5

        p = Param(type='integer', minimum=0, maximum=10)
        p.default = 5
        assert p() == 5

        with pytest.raises(ValueError):
            p.default = 20
            p()

    def test_number_param(self):
        p = Param(42, type='number')
        assert p() == 42

        p = Param(1.0, type='number')
        assert p() == 1.0

        with pytest.raises(ValueError):
            p = Param('42', type='number')

    def test_list_param(self):
        p = Param([1, 2], type='array')
        assert p() == [1, 2]

    def test_dict_param(self):
        p = Param({'a': 1, 'b': 2}, type='object')
        assert p() == {'a': 1, 'b': 2}

    def test_composite_param(self):
        p = Param(type=["string", "number"])
        p.default = "abc"
        assert p() == "abc"
        p.default = 5.0
        assert p() == 5.0

    def test_param_with_description(self):
        p = Param(10, description='Sample description')
        assert p.description == 'Sample description'

    def test_suppress_exception(self):
        p = Param('abc', type='string', minLength=2, maxLength=4)
        assert p() == 'abc'

        p.default = 'long_string'
        assert p(suppress_exception=True) is None

    def test_explicit_schema(self):
        p = Param('abc', schema={type: "string"})
        assert p() == 'abc'

    def test_custom_param(self):
        class S3Param(Param):
            def __init__(self, path: str):
                schema = {"type": "string", "pattern": r"s3:\/\/(.+?)\/(.+)"}
                super().__init__(default=path, schema=schema)

        p = S3Param("s3://my_bucket/my_path")
        assert p() == "s3://my_bucket/my_path"

        with pytest.raises(ValueError):
            p = S3Param("file://not_valid/s3_path")
