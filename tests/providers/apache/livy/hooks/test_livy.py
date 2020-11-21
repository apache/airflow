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

import json
import unittest
from unittest.mock import patch

import requests_mock
from requests.exceptions import RequestException

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.apache.livy.hooks.livy import BatchState, LivyHook
from airflow.utils import db

BATCH_ID = 100
SAMPLE_GET_RESPONSE = {'id': BATCH_ID, 'state': BatchState.SUCCESS.value}


class TestLivyHook(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db.merge_conn(
            Connection(conn_id='livy_default', conn_type='http', host='host', schema='http', port=8998)
        )
        db.merge_conn(Connection(conn_id='default_port', conn_type='http', host='http://host'))
        db.merge_conn(Connection(conn_id='default_protocol', conn_type='http', host='host'))
        db.merge_conn(Connection(conn_id='port_set', host='host', conn_type='http', port=1234))
        db.merge_conn(Connection(conn_id='schema_set', host='host', conn_type='http', schema='zzz'))
        db.merge_conn(
            Connection(conn_id='dont_override_schema', conn_type='http', host='http://host', schema='zzz')
        )
        db.merge_conn(Connection(conn_id='missing_host', conn_type='http', port=1234))
        db.merge_conn(Connection(conn_id='invalid_uri', uri='http://invalid_uri:4321'))

    def test_build_get_hook(self):

        connection_url_mapping = {
            # id, expected
            'default_port': 'http://host',
            'default_protocol': 'http://host',
            'port_set': 'http://host:1234',
            'schema_set': 'zzz://host',
            'dont_override_schema': 'http://host',
        }

        for conn_id, expected in connection_url_mapping.items():
            with self.subTest(conn_id):
                hook = LivyHook(livy_conn_id=conn_id)

                hook.get_conn()
                self.assertEqual(hook.base_url, expected)

    @unittest.skip("inherited HttpHook does not handle missing hostname")
    def test_missing_host(self):
        with self.assertRaises(AirflowException):
            LivyHook(livy_conn_id='missing_host').get_conn()

    def test_build_body(self):
        with self.subTest('minimal request'):
            body = LivyHook.build_post_batch_body(file='appname')

            self.assertEqual(body, {'file': 'appname'})

        with self.subTest('complex request'):
            body = LivyHook.build_post_batch_body(
                file='appname',
                class_name='org.example.livy',
                proxy_user='proxyUser',
                args=['a', '1'],
                jars=['jar1', 'jar2'],
                files=['file1', 'file2'],
                py_files=['py1', 'py2'],
                archives=['arch1', 'arch2'],
                queue='queue',
                name='name',
                conf={'a': 'b'},
                driver_cores=2,
                driver_memory='1M',
                executor_memory='1m',
                executor_cores='1',  # noqa
                num_executors='10',
            )

            self.assertEqual(
                body,
                {
                    'file': 'appname',
                    'className': 'org.example.livy',
                    'proxyUser': 'proxyUser',
                    'args': ['a', '1'],
                    'jars': ['jar1', 'jar2'],
                    'files': ['file1', 'file2'],
                    'pyFiles': ['py1', 'py2'],
                    'archives': ['arch1', 'arch2'],
                    'queue': 'queue',
                    'name': 'name',
                    'conf': {'a': 'b'},
                    'driverCores': 2,
                    'driverMemory': '1M',
                    'executorMemory': '1m',
                    'executorCores': '1',
                    'numExecutors': '10',
                },
            )

    def test_parameters_validation(self):
        with self.subTest('not a size'):
            with self.assertRaises(ValueError):
                LivyHook.build_post_batch_body(file='appname', executor_memory='xxx')

        with self.subTest('list of stringables'):
            self.assertEqual(
                LivyHook.build_post_batch_body(file='appname', args=['a', 1, 0.1])['args'], ['a', '1', '0.1']
            )

    def test_validate_size_format(self):
        with self.subTest('lower 1'):
            self.assertTrue(LivyHook._validate_size_format('1m'))

        with self.subTest('lower 2'):
            self.assertTrue(LivyHook._validate_size_format('1mb'))

        with self.subTest('upper 1'):
            self.assertTrue(LivyHook._validate_size_format('1G'))

        with self.subTest('upper 2'):
            self.assertTrue(LivyHook._validate_size_format('1GB'))

        with self.subTest('snake 1'):
            self.assertTrue(LivyHook._validate_size_format('1Gb'))

        with self.subTest('fullmatch'):
            with self.assertRaises(ValueError):
                self.assertTrue(LivyHook._validate_size_format('1Gb foo'))

        with self.subTest('missing size'):
            with self.assertRaises(ValueError):
                self.assertTrue(LivyHook._validate_size_format('10'))

        with self.subTest('numeric'):
            with self.assertRaises(ValueError):
                LivyHook._validate_size_format(1)  # noqa

        with self.subTest('None'):
            self.assertTrue(LivyHook._validate_size_format(None))  # noqa

    def test_validate_list_of_stringables(self):
        with self.subTest('valid list'):
            try:
                LivyHook._validate_list_of_stringables([1, 'string'])
            except ValueError:
                self.fail("Exception raised")

        with self.subTest('valid tuple'):
            try:
                LivyHook._validate_list_of_stringables((1, 'string'))
            except ValueError:
                self.fail("Exception raised")

        with self.subTest('empty list'):
            try:
                LivyHook._validate_list_of_stringables([])
            except ValueError:
                self.fail("Exception raised")

        with self.subTest('dict'):
            with self.assertRaises(ValueError):
                LivyHook._validate_list_of_stringables({'a': 'a'})

        with self.subTest('invalid element'):
            with self.assertRaises(ValueError):
                LivyHook._validate_list_of_stringables([1, {}])

        with self.subTest('dict'):
            with self.assertRaises(ValueError):
                LivyHook._validate_list_of_stringables([1, None])

        with self.subTest('None'):
            with self.assertRaises(ValueError):
                LivyHook._validate_list_of_stringables(None)  # noqa

        with self.subTest('int'):
            with self.assertRaises(ValueError):
                LivyHook._validate_list_of_stringables(1)  # noqa

        with self.subTest('string'):
            with self.assertRaises(ValueError):
                LivyHook._validate_list_of_stringables('string')

    def test_validate_extra_conf(self):
        with self.subTest('valid'):
            try:
                LivyHook._validate_extra_conf({'k1': 'v1', 'k2': 0})
            except ValueError:
                self.fail("Exception raised")

        with self.subTest('empty dict'):
            try:
                LivyHook._validate_extra_conf({})
            except ValueError:
                self.fail("Exception raised")

        with self.subTest('none'):
            try:
                LivyHook._validate_extra_conf(None)  # noqa
            except ValueError:
                self.fail("Exception raised")

        with self.subTest('not a dict 1'):
            with self.assertRaises(ValueError):
                LivyHook._validate_extra_conf('k1=v1')  # noqa

        with self.subTest('not a dict 2'):
            with self.assertRaises(ValueError):
                LivyHook._validate_extra_conf([('k1', 'v1'), ('k2', 0)])  # noqa

        with self.subTest('nested dict'):
            with self.assertRaises(ValueError):
                LivyHook._validate_extra_conf({'outer': {'inner': 'val'}})

        with self.subTest('empty items'):
            with self.assertRaises(ValueError):
                LivyHook._validate_extra_conf({'has_val': 'val', 'no_val': None})

        with self.subTest('empty string'):
            with self.assertRaises(ValueError):
                LivyHook._validate_extra_conf({'has_val': 'val', 'no_val': ''})

    @patch('airflow.providers.apache.livy.hooks.livy.LivyHook.run_method')
    def test_post_batch_arguments(self, mock_request):

        mock_request.return_value.status_code = 201
        mock_request.return_value.json.return_value = {
            'id': BATCH_ID,
            'state': BatchState.STARTING.value,
            'log': [],
        }

        hook = LivyHook()
        resp = hook.post_batch(file='sparkapp')

        mock_request.assert_called_once_with(
            method='POST', endpoint='/batches', data=json.dumps({'file': 'sparkapp'})
        )

        request_args = mock_request.call_args[1]
        self.assertIn('data', request_args)
        self.assertIsInstance(request_args['data'], str)

        self.assertIsInstance(resp, int)
        self.assertEqual(resp, BATCH_ID)

    @requests_mock.mock()
    def test_post_batch_success(self, mock):
        mock.register_uri(
            'POST',
            '//livy:8998/batches',
            json={'id': BATCH_ID, 'state': BatchState.STARTING.value, 'log': []},
            status_code=201,
        )

        resp = LivyHook().post_batch(file='sparkapp')

        self.assertIsInstance(resp, int)
        self.assertEqual(resp, BATCH_ID)

    @requests_mock.mock()
    def test_post_batch_fail(self, mock):
        mock.register_uri('POST', '//livy:8998/batches', json={}, status_code=400, reason='ERROR')

        hook = LivyHook()
        with self.assertRaises(AirflowException):
            hook.post_batch(file='sparkapp')

    @requests_mock.mock()
    def test_get_batch_success(self, mock):
        mock.register_uri('GET', f'//livy:8998/batches/{BATCH_ID}', json={'id': BATCH_ID}, status_code=200)

        hook = LivyHook()
        resp = hook.get_batch(BATCH_ID)

        self.assertIsInstance(resp, dict)
        self.assertIn('id', resp)

    @requests_mock.mock()
    def test_get_batch_fail(self, mock):
        mock.register_uri(
            'GET',
            f'//livy:8998/batches/{BATCH_ID}',
            json={'msg': 'Unable to find batch'},
            status_code=404,
            reason='ERROR',
        )

        hook = LivyHook()
        with self.assertRaises(AirflowException):
            hook.get_batch(BATCH_ID)

    def test_invalid_uri(self):
        hook = LivyHook(livy_conn_id='invalid_uri')
        with self.assertRaises(RequestException):
            hook.post_batch(file='sparkapp')

    @requests_mock.mock()
    def test_get_batch_state_success(self, mock):

        running = BatchState.RUNNING

        mock.register_uri(
            'GET',
            f'//livy:8998/batches/{BATCH_ID}/state',
            json={'id': BATCH_ID, 'state': running.value},
            status_code=200,
        )

        state = LivyHook().get_batch_state(BATCH_ID)

        self.assertIsInstance(state, BatchState)
        self.assertEqual(state, running)

    @requests_mock.mock()
    def test_get_batch_state_fail(self, mock):
        mock.register_uri(
            'GET', f'//livy:8998/batches/{BATCH_ID}/state', json={}, status_code=400, reason='ERROR'
        )

        hook = LivyHook()
        with self.assertRaises(AirflowException):
            hook.get_batch_state(BATCH_ID)

    @requests_mock.mock()
    def test_get_batch_state_missing(self, mock):
        mock.register_uri('GET', f'//livy:8998/batches/{BATCH_ID}/state', json={}, status_code=200)

        hook = LivyHook()
        with self.assertRaises(AirflowException):
            hook.get_batch_state(BATCH_ID)

    def test_parse_post_response(self):
        res_id = LivyHook._parse_post_response({'id': BATCH_ID, 'log': []})

        self.assertEqual(BATCH_ID, res_id)

    @requests_mock.mock()
    def test_delete_batch_success(self, mock):
        mock.register_uri(
            'DELETE', f'//livy:8998/batches/{BATCH_ID}', json={'msg': 'deleted'}, status_code=200
        )

        resp = LivyHook().delete_batch(BATCH_ID)

        self.assertEqual(resp, {'msg': 'deleted'})

    @requests_mock.mock()
    def test_delete_batch_fail(self, mock):
        mock.register_uri(
            'DELETE', f'//livy:8998/batches/{BATCH_ID}', json={}, status_code=400, reason='ERROR'
        )

        hook = LivyHook()
        with self.assertRaises(AirflowException):
            hook.delete_batch(BATCH_ID)

    @requests_mock.mock()
    def test_missing_batch_id(self, mock):
        mock.register_uri('POST', '//livy:8998/batches', json={}, status_code=201)

        hook = LivyHook()
        with self.assertRaises(AirflowException):
            hook.post_batch(file='sparkapp')

    @requests_mock.mock()
    def test_get_batch_validation(self, mock):
        mock.register_uri('GET', f'//livy:8998/batches/{BATCH_ID}', json=SAMPLE_GET_RESPONSE, status_code=200)

        hook = LivyHook()
        with self.subTest('get_batch'):
            hook.get_batch(BATCH_ID)

        # make sure blocked by validation
        for val in [None, 'one', {'a': 'b'}]:
            with self.subTest(f'get_batch {val}'):
                with self.assertRaises(TypeError):
                    hook.get_batch(val)

    @requests_mock.mock()
    def test_get_batch_state_validation(self, mock):
        mock.register_uri(
            'GET', f'//livy:8998/batches/{BATCH_ID}/state', json=SAMPLE_GET_RESPONSE, status_code=200
        )

        hook = LivyHook()
        with self.subTest('get_batch'):
            hook.get_batch_state(BATCH_ID)

        for val in [None, 'one', {'a': 'b'}]:
            with self.subTest(f'get_batch {val}'):
                with self.assertRaises(TypeError):
                    hook.get_batch_state(val)

    @requests_mock.mock()
    def test_delete_batch_validation(self, mock):
        mock.register_uri('DELETE', f'//livy:8998/batches/{BATCH_ID}', json={'id': BATCH_ID}, status_code=200)

        hook = LivyHook()
        with self.subTest('get_batch'):
            hook.delete_batch(BATCH_ID)

        for val in [None, 'one', {'a': 'b'}]:
            with self.subTest(f'get_batch {val}'):
                with self.assertRaises(TypeError):
                    hook.delete_batch(val)

    def test_check_session_id(self):
        with self.subTest('valid 00'):
            try:
                LivyHook._validate_session_id(100)
            except TypeError:
                self.fail("")

        with self.subTest('valid 01'):
            try:
                LivyHook._validate_session_id(0)
            except TypeError:
                self.fail("")

        with self.subTest('None'):
            with self.assertRaises(TypeError):
                LivyHook._validate_session_id(None)  # noqa

        with self.subTest('random string'):
            with self.assertRaises(TypeError):
                LivyHook._validate_session_id('asd')
