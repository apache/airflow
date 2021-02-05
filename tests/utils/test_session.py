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
#
import sys
import unittest
from unittest import mock

import pytest

from airflow.utils.session import provide_session


def dummy_session(session=None):
    return session


class TestSession(unittest.TestCase):
    def test_raised_provide_session(self):
        with pytest.raises(ValueError, match="Function .*dummy has no `session` argument"):

            @provide_session
            def dummy():
                pass

    @pytest.mark.skipif(sys.version_info < (3, 7, 2), reason="requires python3.7.3 or higher")
    def test_provide_session_without_args_and_kwargs(self):
        mock_dummy = mock.create_autospec(dummy_session)
        mock_dummy()
        args, kwargs = mock_dummy.call_args
        assert not args and not kwargs

        wrapper = provide_session(mock_dummy)

        wrapper()
        args, kwargs = mock_dummy.call_args
        assert not args
        assert 'session' in kwargs and kwargs['session'] is not None

    @pytest.mark.skipif(sys.version_info < (3, 7, 2), reason="requires python3.7.3 or higher")
    def test_provide_session_with_args(self):
        mock_dummy = mock.create_autospec(dummy_session)
        wrapper = provide_session(mock_dummy)

        session = mock.Mock()
        wrapper(session)
        args, kwargs = mock_dummy.call_args
        assert args and args[0] is session
        assert 'session' not in kwargs

    @pytest.mark.skipif(sys.version_info < (3, 7, 2), reason="requires python3.7.3 or higher")
    def test_provide_session_with_kwargs(self):
        mock_dummy = mock.create_autospec(dummy_session)
        wrapper = provide_session(mock_dummy)

        session = mock.Mock()
        wrapper(session=session)
        args, kwargs = mock_dummy.call_args
        assert not args
        assert 'session' in kwargs and kwargs['session'] is session
