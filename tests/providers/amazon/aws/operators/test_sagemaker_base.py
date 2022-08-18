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
from typing import Any, Dict, List

from airflow.providers.amazon.aws.operators.sagemaker import SageMakerBaseOperator

CONFIG: Dict = {'key1': '1', 'key2': {'key3': '3', 'key4': '4'}, 'key5': [{'key6': '6'}, {'key6': '7'}]}
PARSED_CONFIG: Dict = {'key1': 1, 'key2': {'key3': 3, 'key4': 4}, 'key5': [{'key6': 6}, {'key6': 7}]}

EXPECTED_INTEGER_FIELDS: List[List[Any]] = []


class TestSageMakerBaseOperator(unittest.TestCase):
    def setUp(self):
        self.sagemaker = SageMakerBaseOperator(task_id='test_sagemaker_operator', config=CONFIG)
        self.sagemaker.aws_conn_id = 'aws_default'

    def test_parse_integer(self):
        self.sagemaker.integer_fields = [['key1'], ['key2', 'key3'], ['key2', 'key4'], ['key5', 'key6']]
        self.sagemaker.parse_config_integers()
        assert self.sagemaker.config == PARSED_CONFIG

    def test_default_integer_fields(self):
        self.sagemaker.preprocess_config()

        assert self.sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS
