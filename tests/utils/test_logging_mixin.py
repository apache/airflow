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
import warnings
from unittest import mock

from airflow.utils.log.logging_mixin import StreamLogWriter, set_context


class TestLoggingMixin(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings(
            action='always'
        )

    def test_set_context(self):
        handler1 = mock.MagicMock()
        handler2 = mock.MagicMock()
        parent = mock.MagicMock()
        parent.propagate = False
        parent.handlers = [handler1, ]
        log = mock.MagicMock()
        log.handlers = [handler2, ]
        log.parent = parent
        log.propagate = True

        value = "test"
        set_context(log, value)

        handler1.set_context.assert_called_once_with(value)
        handler2.set_context.assert_called_once_with(value)

    def tearDown(self):
        warnings.resetwarnings()


class TestStreamLogWriter(unittest.TestCase):
    def test_write(self):
        logger = mock.MagicMock()
        logger.log = mock.MagicMock()

        log = StreamLogWriter(logger, 1)

        msg = "test_message"
        log.write(msg)

        self.assertEqual(log._buffer, msg)

        log.write(" \n")
        logger.log.assert_called_once_with(1, msg)

        self.assertEqual(log._buffer, "")

    def test_flush(self):
        logger = mock.MagicMock()
        logger.log = mock.MagicMock()

        log = StreamLogWriter(logger, 1)

        msg = "test_message"

        log.write(msg)
        self.assertEqual(log._buffer, msg)

        log.flush()
        logger.log.assert_called_once_with(1, msg)

        self.assertEqual(log._buffer, "")

    def test_isatty(self):
        logger = mock.MagicMock()
        logger.log = mock.MagicMock()

        log = StreamLogWriter(logger, 1)
        self.assertFalse(log.isatty())

    def test_encoding(self):
        logger = mock.MagicMock()
        logger.log = mock.MagicMock()

        log = StreamLogWriter(logger, 1)
        self.assertFalse(log.encoding)
