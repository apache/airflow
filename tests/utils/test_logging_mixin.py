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
from __future__ import annotations

import logging
import warnings
from unittest import mock

import pytest

from airflow.utils.log.logging_mixin import StreamLogWriter, set_context


@pytest.fixture
def logger():
    parent = logging.getLogger(__name__)
    parent.propagate = False
    yield parent

    parent.propagate = True


@pytest.fixture
def child_logger(logger):
    yield logger.getChild("child")


@pytest.fixture
def parent_child_handlers(child_logger):
    parent_handler = logging.NullHandler()
    parent_handler.handle = mock.MagicMock(name="parent_handler.handle")

    child_handler = logging.NullHandler()
    child_handler.handle = mock.MagicMock(name="handler.handle")

    logger = child_logger.parent
    logger.addHandler(parent_handler)

    child_logger.addHandler(child_handler),
    child_logger.propagate = True

    yield parent_handler, child_handler

    logger.removeHandler(parent_handler)
    child_logger.removeHandler(child_handler)


class TestLoggingMixin:
    def setup_method(self):
        warnings.filterwarnings(action="always")

    def test_set_context(self, child_logger, parent_child_handlers):
        h_parent, h_child = parent_child_handlers
        h_parent.set_context = mock.MagicMock()
        h_child.set_context = mock.MagicMock()

        parent = logging.getLogger(__name__)
        parent.propagate = False
        parent.addHandler(h_parent)
        log = parent.getChild("child")
        log.addHandler(h_child),
        log.propagate = True

        value = "test"
        set_context(log, value)

        h_parent.set_context.assert_not_called()
        h_child.set_context.assert_called_once_with(value)

    def teardown_method(self):
        warnings.resetwarnings()


class TestStreamLogWriter:
    def test_write(self):
        logger = mock.MagicMock()
        logger.log = mock.MagicMock()

        log = StreamLogWriter(logger, 1)

        msg = "test_message"
        log.write(msg)

        assert log._buffer == msg

        log.write(" \n")
        logger.log.assert_called_once_with(1, msg)

        assert log._buffer == ""

    def test_flush(self):
        logger = mock.MagicMock()
        logger.log = mock.MagicMock()

        log = StreamLogWriter(logger, 1)

        msg = "test_message"

        log.write(msg)
        assert log._buffer == msg

        log.flush()
        logger.log.assert_called_once_with(1, msg)

        assert log._buffer == ""

    def test_isatty(self):
        logger = mock.MagicMock()
        logger.log = mock.MagicMock()

        log = StreamLogWriter(logger, 1)
        assert not log.isatty()

    def test_encoding(self):
        logger = mock.MagicMock()
        logger.log = mock.MagicMock()

        log = StreamLogWriter(logger, 1)
        assert log.encoding is None

    def test_iobase_compatibility(self):
        log = StreamLogWriter(None, 1)

        assert not log.closed
        # has no specific effect
        log.close()
