# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# 'License'); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest

from airflow.contrib.operators.sagemaker_base_operator import SageMakerBaseOperator

config = {
    'key1': '1',
    'key2': {
        'key3': '3',
        'key4': '4'
    },
    'key5': [
        {
            'key6': '6'
        },
        {
            'key6': '7'
        }
    ]
}

parsed_config = {
    'key1': 1,
    'key2': {
        'key3': 3,
        'key4': 4
    },
    'key5': [
        {
            'key6': 6
        },
        {
            'key6': 7
        }
    ]
}


class TestSageMakerBaseOperator(unittest.TestCase):

    def setUp(self):
        self.sagemaker = SageMakerBaseOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_id',
            config=config
        )

    def test_parse_integer(self):
        self.sagemaker.integer_fields = [
            ['key1'], ['key2', 'key3'], ['key2', 'key4'], ['key5', 'key6']
        ]
        self.sagemaker.parse_config_integers()
        self.assertEqual(self.sagemaker.config, parsed_config)


if __name__ == '__main__':
    unittest.main()
