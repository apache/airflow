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

import sys
import unittest
import warnings
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

import pytest

from airflow.utils.log.logging_mixin import (
    StderrToLog, StdoutToLog, StreamLogWriter, _StreamToLogRedirector, set_context,
)


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

        log.write(" \n")
        logger.log.assert_called_once_with(1, msg)

        assert isinstance(log._buffer, list)
        assert not log._buffer

    def test_flush(self):
        logger = mock.MagicMock()
        logger.log = mock.MagicMock()

        log = StreamLogWriter(logger, 1)

        msg = "test_message"

        log.write(msg)

        log.flush()
        logger.log.assert_called_once_with(1, msg)

        assert isinstance(log._buffer, list)
        assert not log._buffer

    def test_isatty(self):
        logger = mock.MagicMock()
        logger.log = mock.MagicMock()

        log = StreamLogWriter(logger, 1)
        self.assertFalse(log.isatty())

    def test_encoding(self):
        logger = mock.MagicMock()
        logger.log = mock.MagicMock()

        log = StreamLogWriter(logger, 1)
        self.assertIsNone(log.encoding)

    def test_add_stream_target(self):
        logger = mock.MagicMock()
        logger.log = mock.MagicMock()

        stream_mock = mock.MagicMock()
        stream_mock.write = mock.MagicMock()

        log = StreamLogWriter(logger, 1)
        log.add_stream_target(stream_mock)

        msg = "test_message"
        log.write(msg + '\n')

        # check that the message was propagated to the logger:
        logger.log.assert_called_once_with(1, msg)

        # check that the message was propagated to the added stream:
        stream_mock.write.assert_called_once_with(msg + '\n')


@pytest.fixture()
def mock_logger():
    logger = mock.MagicMock()
    logger.log = mock.MagicMock()
    return logger


@pytest.fixture()
def mock_stream():
    stream = mock.MagicMock()
    stream.write = mock.MagicMock()
    return stream


class TestStreamToLogRedirector:
    def test_initialization(self, mock_logger):
        instance = _StreamToLogRedirector(mock_logger, 10, False)
        assert isinstance(instance._replacement_stream, StreamLogWriter)


class TestStdoutToLog:
    def test_context(self, mock_logger):
        existing_stream = sys.stdout

        with StdoutToLog(mock_logger, 10):
            assert isinstance(sys.stdout, StreamLogWriter)
        assert sys.stdout is existing_stream

    def test_propagate(self, mock_logger, mock_stream):
        existing_stream = sys.stdout
        msg = 'this is a test'

        # mock the existing stdout so that we can spy on it
        with redirect_stdout(mock_stream):
            with StdoutToLog(mock_logger, 10, True):
                sys.stdout.write(msg)
        mock_stream.write.called_once_with(msg)
        mock_logger.log.assert_called_once_with(10, msg)

        # check that everything is undone:
        assert sys.stdout is existing_stream


class TestStderrToLog:
    def test_context(self, mock_logger):
        existing_stream = sys.stderr
        with StderrToLog(mock_logger, 10):
            assert isinstance(sys.stderr, StreamLogWriter)
        assert sys.stderr is existing_stream

    def test_propagate(self, mock_logger, mock_stream):
        existing_stream = sys.stderr
        msg = 'this is an error'

        # mock the existing stderr so that we can spy on it
        with redirect_stderr(mock_stream):
            with StderrToLog(mock_logger, 10, True):
                sys.stderr.write(msg)
        mock_stream.write.assert_called_once_with(msg)
        mock_logger.log.assert_called_once_with(10, msg)

        # check that everything is undone:
        assert sys.stderr is existing_stream
