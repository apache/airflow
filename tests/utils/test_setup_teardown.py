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

import pytest

from airflow.exceptions import AirflowException
from airflow.utils.setup_teardown import SetupTeardownContext


class TestSetupTearDownContext:
    def test_setup(self):
        assert SetupTeardownContext.is_setup is False
        assert SetupTeardownContext.is_teardown is False

        with SetupTeardownContext.setup():
            assert SetupTeardownContext.is_setup is True
            assert SetupTeardownContext.is_teardown is False

        assert SetupTeardownContext.is_setup is False
        assert SetupTeardownContext.is_teardown is False

    def test_teardown(self):
        assert SetupTeardownContext.is_setup is False
        assert SetupTeardownContext.is_teardown is False

        with SetupTeardownContext.setup():
            assert SetupTeardownContext.is_setup is True
            assert SetupTeardownContext.is_teardown is False

        assert SetupTeardownContext.is_setup is False
        assert SetupTeardownContext.is_teardown is False

    def test_setup_exception(self):
        """Ensure context is reset even if an exception happens"""
        with pytest.raises(Exception, match="Hello"):
            with SetupTeardownContext.setup():
                raise Exception("Hello")

        assert SetupTeardownContext.is_setup is False
        assert SetupTeardownContext.is_teardown is False

    def test_teardown_exception(self):
        """Ensure context is reset even if an exception happens"""
        with pytest.raises(Exception, match="Hello"):
            with SetupTeardownContext.teardown():
                raise Exception("Hello")

        assert SetupTeardownContext.is_setup is False
        assert SetupTeardownContext.is_teardown is False

    def test_setup_block_nested(self):
        with SetupTeardownContext.setup():
            with pytest.raises(
                AirflowException,
                match=(
                    "A setup task or taskgroup cannot be nested inside another"
                    " setup/teardown task or taskgroup"
                ),
            ):
                with SetupTeardownContext.setup():
                    raise Exception("This should not be reached")

        assert SetupTeardownContext.is_setup is False
        assert SetupTeardownContext.is_teardown is False

    def test_teardown_block_nested(self):
        with SetupTeardownContext.teardown():
            with pytest.raises(
                AirflowException,
                match=(
                    "A teardown task or taskgroup cannot be nested inside another"
                    " setup/teardown task or taskgroup"
                ),
            ):
                with SetupTeardownContext.teardown():
                    raise Exception("This should not be reached")

        assert SetupTeardownContext.is_setup is False
        assert SetupTeardownContext.is_teardown is False

    def test_teardown_nested_in_setup_blocked(self):
        with SetupTeardownContext.setup():
            with pytest.raises(
                AirflowException,
                match=(
                    "A teardown task or taskgroup cannot be nested inside another"
                    " setup/teardown task or taskgroup"
                ),
            ):
                with SetupTeardownContext.teardown():
                    raise Exception("This should not be reached")

        assert SetupTeardownContext.is_setup is False
        assert SetupTeardownContext.is_teardown is False

    def test_setup_nested_in_teardown_blocked(self):
        with SetupTeardownContext.teardown():
            with pytest.raises(
                AirflowException,
                match=(
                    "A setup task or taskgroup cannot be nested inside another"
                    " setup/teardown task or taskgroup"
                ),
            ):
                with SetupTeardownContext.setup():
                    raise Exception("This should not be reached")

        assert SetupTeardownContext.is_setup is False
        assert SetupTeardownContext.is_teardown is False
