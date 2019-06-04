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

import os
import shutil
import unittest

import elasticsearch
from unittest import mock
import pendulum

from airflow import configuration
from airflow.models import TaskInstance, DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from airflow.utils.log.es_task_handler import ElasticsearchTaskHandler
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from .elasticmock import elasticmock


class TestElasticsearchTaskHandler(unittest.TestCase):
    DAG_ID = 'dag_for_testing_file_task_handler'
    TASK_ID = 'task_for_testing_file_log_handler'
    EXECUTION_DATE = datetime(2016, 1, 1)
    LOG_ID = '{dag_id}-{task_id}-2016-01-01T00:00:00+00:00-1'.format(dag_id=DAG_ID, task_id=TASK_ID)

    @elasticmock
    def setUp(self):
        super().setUp()
        self.local_log_location = 'local/log/location'
        self.filename_template = '{try_number}.log'
        self.log_id_template = '{dag_id}-{task_id}-{execution_date}-{try_number}'
        self.end_of_log_mark = 'end_of_log\n'
        self.write_stdout = False
        self.json_format = False
        self.json_fields = 'asctime,filename,lineno,levelname,message'
        self.es_task_handler = ElasticsearchTaskHandler(
            self.local_log_location,
            self.filename_template,
            self.log_id_template,
            self.end_of_log_mark,
            self.write_stdout,
            self.json_format,
            self.json_fields
        )

        self.es = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
        self.index_name = 'test_index'
        self.doc_type = 'log'
        self.test_message = 'some random stuff'
        self.body = {'message': self.test_message, 'log_id': self.LOG_ID,
                     'offset': 1}

        self.es.index(index=self.index_name, doc_type=self.doc_type,
                      body=self.body, id=1)

        configuration.load_test_config()
        self.dag = DAG(self.DAG_ID, start_date=self.EXECUTION_DATE)
        task = DummyOperator(task_id=self.TASK_ID, dag=self.dag)
        self.ti = TaskInstance(task=task, execution_date=self.EXECUTION_DATE)
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        self.addCleanup(self.dag.clear)

    def tearDown(self):
        shutil.rmtree(self.local_log_location.split(os.path.sep)[0], ignore_errors=True)

    def test_client(self):
        self.assertIsInstance(self.es_task_handler.client, elasticsearch.Elasticsearch)

    def test_read(self):
        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(self.ti,
                                                    1,
                                                    {'offset': 0,
                                                     'last_log_timestamp': str(ts),
                                                     'end_of_log': False})

        self.assertEqual(1, len(logs))
        self.assertEqual(len(logs), len(metadatas))
        self.assertEqual(self.test_message, logs[0])
        self.assertFalse(metadatas[0]['end_of_log'])
        self.assertEqual('1', metadatas[0]['offset'])
        self.assertTrue(timezone.parse(metadatas[0]['last_log_timestamp']) > ts)

    def test_read_with_match_phrase_query(self):
        similar_log_id = '{task_id}-{dag_id}-2016-01-01T00:00:00+00:00-1'.format(
            dag_id=TestElasticsearchTaskHandler.DAG_ID,
            task_id=TestElasticsearchTaskHandler.TASK_ID)
        another_test_message = 'another message'

        another_body = {'message': another_test_message, 'log_id': similar_log_id, 'offset': 1}
        self.es.index(index=self.index_name, doc_type=self.doc_type,
                      body=another_body, id=1)

        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(self.ti,
                                                    1,
                                                    {'offset': 0,
                                                     'last_log_timestamp': str(ts),
                                                     'end_of_log': False})
        self.assertEqual(1, len(logs))
        self.assertEqual(len(logs), len(metadatas))
        self.assertEqual(self.test_message, logs[0])
        self.assertNotEqual(another_test_message, logs[0])

        self.assertFalse(metadatas[0]['end_of_log'])
        self.assertEqual('1', metadatas[0]['offset'])
        self.assertTrue(timezone.parse(metadatas[0]['last_log_timestamp']) > ts)

    def test_read_with_none_meatadata(self):
        logs, metadatas = self.es_task_handler.read(self.ti, 1)
        self.assertEqual(1, len(logs))
        self.assertEqual(len(logs), len(metadatas))
        self.assertEqual(self.test_message, logs[0])
        self.assertFalse(metadatas[0]['end_of_log'])
        self.assertEqual('1', metadatas[0]['offset'])
        self.assertTrue(
            timezone.parse(metadatas[0]['last_log_timestamp']) < pendulum.now())

    def test_read_nonexistent_log(self):
        ts = pendulum.now()
        # In ElasticMock, search is going to return all documents with matching index
        # and doc_type regardless of match filters, so we delete the log entry instead
        # of making a new TaskInstance to query.
        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=1)
        logs, metadatas = self.es_task_handler.read(self.ti,
                                                    1,
                                                    {'offset': 0,
                                                     'last_log_timestamp': str(ts),
                                                     'end_of_log': False})
        self.assertEqual(1, len(logs))
        self.assertEqual(len(logs), len(metadatas))
        self.assertEqual([''], logs)
        self.assertFalse(metadatas[0]['end_of_log'])
        self.assertEqual('0', metadatas[0]['offset'])
        # last_log_timestamp won't change if no log lines read.
        self.assertTrue(timezone.parse(metadatas[0]['last_log_timestamp']) == ts)

    def test_read_with_empty_metadata(self):
        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(self.ti, 1, {})
        self.assertEqual(1, len(logs))
        self.assertEqual(len(logs), len(metadatas))
        self.assertEqual(self.test_message, logs[0])
        self.assertFalse(metadatas[0]['end_of_log'])
        # offset should be initialized to 0 if not provided.
        self.assertEqual('1', metadatas[0]['offset'])
        # last_log_timestamp will be initialized using log reading time
        # if not last_log_timestamp is provided.
        self.assertTrue(timezone.parse(metadatas[0]['last_log_timestamp']) > ts)

        # case where offset is missing but metadata not empty.
        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=1)
        logs, metadatas = self.es_task_handler.read(self.ti, 1, {'end_of_log': False})
        self.assertEqual(1, len(logs))
        self.assertEqual(len(logs), len(metadatas))
        self.assertEqual([''], logs)
        self.assertFalse(metadatas[0]['end_of_log'])
        # offset should be initialized to 0 if not provided.
        self.assertEqual('0', metadatas[0]['offset'])
        # last_log_timestamp will be initialized using log reading time
        # if not last_log_timestamp is provided.
        self.assertTrue(timezone.parse(metadatas[0]['last_log_timestamp']) > ts)

    def test_read_timeout(self):
        ts = pendulum.now().subtract(minutes=5)

        self.es.delete(index=self.index_name, doc_type=self.doc_type, id=1)
        logs, metadatas = self.es_task_handler.read(self.ti,
                                                    1,
                                                    {'offset': 0,
                                                     'last_log_timestamp': str(ts),
                                                     'end_of_log': False})
        self.assertEqual(1, len(logs))
        self.assertEqual(len(logs), len(metadatas))
        self.assertEqual([''], logs)
        self.assertTrue(metadatas[0]['end_of_log'])
        # offset should be initialized to 0 if not provided.
        self.assertEqual('0', metadatas[0]['offset'])
        self.assertTrue(timezone.parse(metadatas[0]['last_log_timestamp']) == ts)

    def test_read_as_download_logs(self):
        ts = pendulum.now()
        logs, metadatas = self.es_task_handler.read(self.ti,
                                                    1,
                                                    {'offset': 0,
                                                     'last_log_timestamp': str(ts),
                                                     'download_logs': True,
                                                     'end_of_log': False})
        self.assertEqual(1, len(logs))
        self.assertEqual(len(logs), len(metadatas))
        self.assertEqual(self.test_message, logs[0])
        self.assertFalse(metadatas[0]['end_of_log'])
        self.assertTrue(metadatas[0]['download_logs'])
        self.assertEqual('1', metadatas[0]['offset'])
        self.assertTrue(timezone.parse(metadatas[0]['last_log_timestamp']) > ts)

    def test_read_raises(self):
        with mock.patch.object(self.es_task_handler.log, 'exception') as mock_exception:
            with mock.patch("elasticsearch_dsl.Search.execute") as mock_execute:
                mock_execute.side_effect = Exception('Failed to read')
                logs, metadatas = self.es_task_handler.read(self.ti, 1)
            assert mock_exception.call_count == 1
            args, kwargs = mock_exception.call_args
            self.assertIn("Could not read log with log_id:", args[0])

        self.assertEqual(1, len(logs))
        self.assertEqual(len(logs), len(metadatas))
        self.assertEqual([''], logs)
        self.assertFalse(metadatas[0]['end_of_log'])
        self.assertEqual('0', metadatas[0]['offset'])

    def test_set_context(self):
        self.es_task_handler.set_context(self.ti)
        self.assertTrue(self.es_task_handler.mark_end_on_close)

    def test_close(self):
        self.es_task_handler.set_context(self.ti)
        self.es_task_handler.close()
        with open(os.path.join(self.local_log_location,
                               self.filename_template.format(try_number=1)),
                  'r') as log_file:
            self.assertIn(self.end_of_log_mark, log_file.read())
        self.assertTrue(self.es_task_handler.closed)

    def test_close_no_mark_end(self):
        self.ti.raw = True
        self.es_task_handler.set_context(self.ti)
        self.es_task_handler.close()
        with open(os.path.join(self.local_log_location,
                               self.filename_template.format(try_number=1)),
                  'r') as log_file:
            self.assertNotIn(self.end_of_log_mark, log_file.read())
        self.assertTrue(self.es_task_handler.closed)

    def test_close_closed(self):
        self.es_task_handler.closed = True
        self.es_task_handler.set_context(self.ti)
        self.es_task_handler.close()
        with open(os.path.join(self.local_log_location,
                               self.filename_template.format(try_number=1)),
                  'r') as log_file:
            self.assertEqual(0, len(log_file.read()))

    def test_close_with_no_handler(self):
        self.es_task_handler.set_context(self.ti)
        self.es_task_handler.handler = None
        self.es_task_handler.close()
        with open(os.path.join(self.local_log_location,
                               self.filename_template.format(try_number=1)),
                  'r') as log_file:
            self.assertEqual(0, len(log_file.read()))
        self.assertTrue(self.es_task_handler.closed)

    def test_close_with_no_stream(self):
        self.es_task_handler.set_context(self.ti)
        self.es_task_handler.handler.stream = None
        self.es_task_handler.close()
        with open(os.path.join(self.local_log_location,
                               self.filename_template.format(try_number=1)),
                  'r') as log_file:
            self.assertIn(self.end_of_log_mark, log_file.read())
        self.assertTrue(self.es_task_handler.closed)

        self.es_task_handler.set_context(self.ti)
        self.es_task_handler.handler.stream.close()
        self.es_task_handler.close()
        with open(os.path.join(self.local_log_location,
                               self.filename_template.format(try_number=1)),
                  'r') as log_file:
            self.assertIn(self.end_of_log_mark, log_file.read())
        self.assertTrue(self.es_task_handler.closed)

    def test_render_log_id(self):
        expected_log_id = 'dag_for_testing_file_task_handler-' \
                          'task_for_testing_file_log_handler-2016-01-01T00:00:00+00:00-1'
        log_id = self.es_task_handler._render_log_id(self.ti, 1)
        self.assertEqual(expected_log_id, log_id)

        # Switch to use jinja template.
        self.es_task_handler = ElasticsearchTaskHandler(
            self.local_log_location,
            self.filename_template,
            '{{ ti.dag_id }}-{{ ti.task_id }}-{{ ts }}-{{ try_number }}',
            self.end_of_log_mark,
            self.write_stdout,
            self.json_format,
            self.json_fields
        )
        log_id = self.es_task_handler._render_log_id(self.ti, 1)
        self.assertEqual(expected_log_id, log_id)

    def test_clean_execution_date(self):
        clean_execution_date = self.es_task_handler._clean_execution_date(self.EXECUTION_DATE)
        self.assertEqual('2016_01_01T00_12_00_000000', clean_execution_date)
