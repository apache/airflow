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
from mock import patch

from airflow import configuration
from airflow.contrib.hooks.redis_hook import RedisHook


class TestRedisHook(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

    def test_get_conn(self):
        hook = RedisHook(redis_conn_id='redis_default')
        self.assertEqual(hook.client, None)
        self.assertEqual(
            repr(hook.get_conn()),
            (
                'Redis<ConnectionPool'
                '<Connection<host=localhost,port=6379,db=0>>>'
            )
        )

    @patch("airflow.contrib.hooks.redis_hook.RedisHook.get_conn")
    def test_first_conn_instantiation(self, get_conn):
        hook = RedisHook(redis_conn_id='redis_default')
        hook.key_exists('test_key')
        self.assertTrue(get_conn.called_once())


if __name__ == '__main__':
    unittest.main()
