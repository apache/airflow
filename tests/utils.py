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
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import unittest
from io import StringIO
import os

import airflow.utils.logging as logging_utils
from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.utils.operator_resources import Resources

_log = logging.getLogger('airflow')


class LogUtilsTest(unittest.TestCase):

    def test_gcs_url_parse(self):
        """
        Test GCS url parsing
        """
        _log.info(
            'About to create a GCSLog object without a connection. This will '
            'log an error but testing will proceed.')
        glog = logging_utils.GCSLog()

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


class LoggingHandlerSetupTests(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger('airflow.test')

    def tearDown(self):
        if self.handler:
            self.logger.removeHandler(self.handler)

    def test_setup_stream_logging(self):
        # Make sure our handler is getting messages.
        self.handler = logging_utils.setup_stream_logging(self.logger)
        stream = StringIO()
        self.handler.stream = stream  # Override stderr default stream.
        self.logger.info("test message")
        self.assertIn("test message", stream.getvalue())

    def test_setup_file_logging(self):
        filename = 'setup_file_logging_test.log'
        base_log_folder = os.path.expanduser(
            configuration.get('core', 'BASE_LOG_FOLDER'))
        file_path = os.path.join(base_log_folder, filename)
        self.handler = logging_utils.setup_file_logging(self.logger,
                                                        filename)
        self.logger.info("test message")
        with open(file_path, "r") as log_file:
            log_file.seek(0)
            log_message = log_file.read()
            self.assertIn("test message", log_message)


class LoggingControllerTests(unittest.TestCase):

    def setUp(self):
        self.logger = logging.root.getChild('airflow')
        self.existing_handlers = self.logger.handlers
        self.logger.handlers = []
        self.base_log_folder = os.path.expanduser(
            configuration.get('core', 'BASE_LOG_FOLDER'))

        # Use the file paths that are used by the Logging Controller.
        self.debug_log_file_path = '{}/debug.log'.format(self.base_log_folder)
        self.error_log_file_path = '{}/error.log'.format(self.base_log_folder)

        # Ensure the log files are empty.
        open(self.debug_log_file_path, 'w').close()
        open(self.error_log_file_path, 'w').close()

    def tearDown(self):
        self.logger.handlers = self.existing_handlers

        # Ensure the log files are empty.
        open(self.debug_log_file_path, 'w').close()
        open(self.error_log_file_path, 'w').close()

    def test_enable_debug_file_log(self):
        logging_utils.logging_controller.enable_debug_file_log()
        logger = logging.getLogger('airflow.test')
        logger.info("debug file test")
        with open(self.debug_log_file_path, "r") as log_file:
            log_file.seek(0)
            log_message = log_file.read()
        log_file.close()
        self.assertIn("debug file test", log_message)

    def test_enable_error_file_log(self):
        logging_utils.logging_controller.enable_error_file_log()
        logger = logging.getLogger('airflow.test')
        logger.error("error file test")
        with open(self.error_log_file_path, "r") as log_file:
            log_file.seek(0)
            log_message = log_file.read()
        log_file.close()
        self.assertIn("error file test", log_message)

    def test_enable_console_log(self):
        stream = StringIO()
        logging_utils.logging_controller.enable_console_log()

        # Redirect the stream so we can check output
        logging_utils.logging_controller._console_log_handler
        logging_utils.logging_controller._console_log_handler.stream = stream

        logger = logging.getLogger('airflow.test')
        logger.info(u'console test')
        self.assertIn("console test", stream.getvalue())

    def test_disable_debug_file_log(self):
        file_handler = logging_utils.logging_controller._debug_file_log_handler
        logging_utils.logging_controller._logger.addHandler(file_handler)
        logging_utils.logging_controller.disable_debug_file_log()
        self.assertFalse(self.logger.handlers)

    def test_disable_error_file_log(self):
        file_handler = logging_utils.logging_controller._error_file_log_handler
        logging_utils.logging_controller._logger.addHandler(file_handler)
        logging_utils.logging_controller.disable_error_file_log()
        self.assertFalse(self.logger.handlers)

    def test_disable_console_log(self):
        stream_handler = logging_utils.logging_controller._console_log_handler
        logging_utils.logging_controller._logger.addHandler(stream_handler)
        logging_utils.logging_controller.disable_console_log()
        self.assertFalse(self.logger.handlers)

    def test_clear_handlers(self):
        debug_file_handler = \
            logging_utils.logging_controller._debug_file_log_handler
        logging_utils.logging_controller._logger.addHandler(debug_file_handler)
        
        error_file_handler = \
            logging_utils.logging_controller._error_file_log_handler
        logging_utils.logging_controller._logger.addHandler(error_file_handler)

        stream_handler = logging_utils.logging_controller._console_log_handler
        logging_utils.logging_controller._logger.addHandler(
            logging_utils.logging_controller._console_log_handler)

        self.logger.addHandler(logging.FileHandler(
            self.debug_log_file_path))
        self.logger.addHandler(logging.FileHandler(
            self.error_log_file_path))
        self.logger.addHandler(stream_handler)
        logging_utils.logging_controller.clear_handlers()
        self.assertFalse(self.logger.handlers)


class LoggingMixinTest(unittest.TestCase):
    def test(self):
        class MyLoggingClass(logging_utils.LoggingMixin):
            pass

        log_class = MyLoggingClass()
        self.assertEqual(log_class.logger.name, 'tests.utils.MyLoggingClass')


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
                         configuration.getint('operators', 'default_ram'))
        self.assertEqual(resources.disk.qty, 1)
        self.assertEqual(resources.gpus.qty,
                         configuration.getint('operators', 'default_gpus'))

    def test_no_resources_specified(self):
        resources = Resources()
        self.assertEqual(resources.cpus.qty,
                         configuration.getint('operators', 'default_cpus'))
        self.assertEqual(resources.ram.qty,
                         configuration.getint('operators', 'default_ram'))
        self.assertEqual(resources.disk.qty,
                         configuration.getint('operators', 'default_disk'))
        self.assertEqual(resources.gpus.qty,
                         configuration.getint('operators', 'default_gpus'))

    def test_negative_resource_qty(self):
        with self.assertRaises(AirflowException):
            Resources(cpus=-1)
