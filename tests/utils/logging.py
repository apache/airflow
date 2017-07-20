# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

import logging
import unittest

from airflow.utils.logging import GCSLog, HDFSLog
from airflow.exceptions import AirflowException


class GCSLogTest(unittest.TestCase):

    def test_gcs_url_parse(self):
        """
        Test GCS url parsing
        """
        logging.info(
            'About to create a GCSLog object without a connection. This will '
            'log an error, but testing will proceed.')
        glog = GCSLog()

        self.assertEqual(
            glog.parse_gcs_url('gs://bucket/path/to/blob'),
            ('bucket', 'path/to/blob'))

        # invalid URI
        self.assertRaises(
            AirflowException,
            glog.parse_gcs_url,
            'gs:/bucket/path/to/blob')

        # trailing slash
        self.assertEqual(
            glog.parse_gcs_url('gs://bucket/path/to/blob/'),
            ('bucket', 'path/to/blob'))

        # bucket only
        self.assertEqual(
            glog.parse_gcs_url('gs://bucket/'),
            ('bucket', ''))


class HDFSLogTest(unittest.TestCase):

    def test_hdfs_escape_filename(self):
        """
        Test HDFS escape filename
        """
        logging.info(
            'About to create an HDFSLog object without a connection. This '
            'will log an error, but testing will proceed.')
        hdfs_log = HDFSLog()

        path = hdfs_log.escape_filename(
            '/logs/airflow/my_dag/task_1/2017-01-01T00:00:00')
        self.assertEqual(
            '/logs/airflow/my_dag/task_1/2017-01-01T00-00-00', path)

    def test_hdfs_remove_scheme(self):
        """
        Test HDFS escape filename
        """
        logging.info(
            'About to create an HDFSLog object without a connection. This '
            'will log an error, but testing will proceed.')
        hdfs_log = HDFSLog()

        # with scheme
        path = hdfs_log.remove_scheme(
            'hdfs:///logs/airflow/my_dag/task_1/2017-01-01T00:00:00')
        self.assertEqual(
            '/logs/airflow/my_dag/task_1/2017-01-01T00:00:00', path)

        # no scheme
        path = hdfs_log.remove_scheme(
            '/logs/airflow/my_dag/task_1/2017-01-01T00:00:00')
        self.assertEqual(
            '/logs/airflow/my_dag/task_1/2017-01-01T00:00:00', path)
