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

import unittest
from unittest import mock

from sqlalchemy.exc import OperationalError

from airflow.utils.retries import retry_db_transaction


class TestRetries(unittest.TestCase):
    def test_retry_db_transaction_with_passing_retries(self):
        """Test that retries can be passed to decorator"""
        mock_obj = mock.MagicMock()
        mock_session = mock.MagicMock()
        op_error = OperationalError(statement=mock.ANY, params=mock.ANY, orig=mock.ANY)

        @retry_db_transaction(retries=2)
        def test_function(session):
            session.execute("select 1")
            mock_obj(2)
            raise op_error

        with self.assertRaises(OperationalError):
            test_function(session=mock_session)

        assert mock_obj.call_count == 2

    def test_retry_db_transaction_with_default_retries(self):
        """Test that by default 3 retries will be carried out"""
        mock_obj = mock.MagicMock()
        mock_session = mock.MagicMock()
        mock_rollback = mock.MagicMock()
        mock_session.rollback = mock_rollback
        op_error = OperationalError(statement=mock.ANY, params=mock.ANY, orig=mock.ANY)

        @retry_db_transaction
        def test_function(session):
            session.execute("select 1")
            mock_obj(2)
            raise op_error

        with self.assertRaises(OperationalError), self.assertLogs(self.__module__, 'DEBUG') as logs_output:
            test_function(session=mock_session)

        assert (
            "DEBUG:tests.utils.test_retries:Running "
            "TestRetries.test_retry_db_transaction_with_default_retries.<locals>.test_function "
            "with retries. Try 1 of 3" in logs_output.output
        )
        assert (
            "DEBUG:tests.utils.test_retries:Running "
            "TestRetries.test_retry_db_transaction_with_default_retries.<locals>.test_function "
            "with retries. Try 2 of 3" in logs_output.output
        )
        assert (
            "DEBUG:tests.utils.test_retries:Running "
            "TestRetries.test_retry_db_transaction_with_default_retries.<locals>.test_function "
            "with retries. Try 3 of 3" in logs_output.output
        )

        assert mock_session.execute.call_count == 3
        assert mock_rollback.call_count == 3
        mock_rollback.assert_has_calls([mock.call(), mock.call(), mock.call()])

    def test_retry_db_transaction_fails_when_used_in_function_without_retry(self):
        """Test that an error is raised when the decorator is used on a function without session arg"""

        with self.assertRaisesRegex(ValueError, "has no `session` argument"):

            @retry_db_transaction
            def test_function():
                print("hi")
                raise OperationalError(statement=mock.ANY, params=mock.ANY, orig=mock.ANY)

    def test_retry_db_transaction_fails_when_session_not_passed(self):
        """Test that an error is raised when session is not passed to the function"""

        @retry_db_transaction
        def test_function(session):
            session.execute("select 1;")
            raise OperationalError(statement=mock.ANY, params=mock.ANY, orig=mock.ANY)

        with self.assertRaisesRegex(
            TypeError, f"session is a required argument for {test_function.__qualname__}"
        ):
            test_function()
