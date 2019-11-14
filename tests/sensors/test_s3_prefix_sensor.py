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

from airflow.sensors.s3_prefix_sensor import S3PrefixSensor


class TestS3PrefixSensor(unittest.TestCase):

    @mock.patch('airflow.providers.amazon.aws.hooks.s3.S3Hook')
    def test_poke(self, mock_hook):
        s = S3PrefixSensor(
            task_id='s3_prefix',
            bucket_name='bucket',
            prefix='prefix')

        mock_hook.return_value.check_for_prefix.return_value = False
        self.assertFalse(s.poke(None))
        mock_hook.return_value.check_for_prefix.assert_called_once_with(
            prefix='prefix',
            delimiter='/',
            bucket_name='bucket')

        mock_hook.return_value.check_for_prefix.return_value = True
        self.assertTrue(s.poke(None))
