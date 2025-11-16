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

import gc
from unittest import mock

import pytest

from airflow.utils.gc_utils import with_gc_freeze


class TestWithGcFreeze:
    @mock.patch.object(gc, "unfreeze")
    @mock.patch.object(gc, "freeze")
    def test_gc_freeze_and_unfreeze_called(self, freeze_mock, unfreeze_mock):
        """Test that gc.freeze and gc.unfreeze are called"""

        @with_gc_freeze
        def dummy_function():
            return "success"

        result = dummy_function()

        assert result == "success"
        freeze_mock.assert_called_once()
        unfreeze_mock.assert_called_once()

    @mock.patch.object(gc, "unfreeze")
    @mock.patch.object(gc, "freeze")
    def test_unfreeze_called_even_on_exception(self, freeze_mock, unfreeze_mock):
        """Test that gc.unfreeze is called even when an exception occurs"""

        @with_gc_freeze
        def failing_function():
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            failing_function()

        freeze_mock.assert_called_once()
        unfreeze_mock.assert_called_once()

    @mock.patch.object(gc, "unfreeze")
    @mock.patch.object(gc, "freeze")
    def test_function_arguments_passed_correctly(self, freeze_mock, unfreeze_mock):
        """Test that function arguments are passed correctly"""

        @with_gc_freeze
        def function_with_args(a, b, c=None):
            return a + b + (c or 0)

        result = function_with_args(1, 2, c=3)

        assert result == 6
        freeze_mock.assert_called_once()
        unfreeze_mock.assert_called_once()
