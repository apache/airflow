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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import unittest

import airflow.utils.logging
from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.utils.operator_resources import Resources


class LogUtilsTest(unittest.TestCase):

    def test_gcs_url_parse(self):
        """
        Test GCS url parsing
        """
        logging.info(
            'About to create a GCSLog object without a connection. This will '
            'log an error but testing will proceed.')
        glog = airflow.utils.logging.GCSLog()

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


class OperatorResourcesTest(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()

    def test_all_resources_specified(self):
        resources = Resources(cpus=1, ram=2, disk=3, gpus=4)
        self.assertEqual(resources.cpus.qty, 1)
        self.assertEqual(resources.ram.qty, 2)
        self.assertEqual(resources.disk.qty, 3)
        self.assertEqual(resources.gpus.qty, 4)

    def test_some_resources_specified(self):
        resources = Resources(cpus=0, disk=1)
        self.assertEqual(resources.cpus.qty, 0)
        self.assertEqual(resources.ram.qty,
                         configuration.conf.getint('operators', 'default_ram'))
        self.assertEqual(resources.disk.qty, 1)
        self.assertEqual(resources.gpus.qty,
                         configuration.conf.getint('operators', 'default_gpus'))

    def test_no_resources_specified(self):
        resources = Resources()
        self.assertEqual(resources.cpus.qty,
                         configuration.conf.getint('operators', 'default_cpus'))
        self.assertEqual(resources.ram.qty,
                         configuration.conf.getint('operators', 'default_ram'))
        self.assertEqual(resources.disk.qty,
                         configuration.conf.getint('operators', 'default_disk'))
        self.assertEqual(resources.gpus.qty,
                         configuration.conf.getint('operators', 'default_gpus'))

    def test_negative_resource_qty(self):
        with self.assertRaises(AirflowException):
            Resources(cpus=-1)
