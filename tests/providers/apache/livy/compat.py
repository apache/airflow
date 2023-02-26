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

__all__ = ["async_mock", "AsyncMock"]

import sys

if sys.version_info < (3, 8):
    # For compatibility with Python 3.7
    from asynctest import mock as async_mock

    # ``asynctest.mock.CoroutineMock`` which provide compatibility not working well with autospec=True
    # as result "TypeError: object MagicMock can't be used in 'await' expression" could be raised.
    # Best solution in this case provide as spec actual awaitable object
    # >>> from tests.providers.apache.livy.compat import AsyncMock
    # >>> from foo.bar import SpamEgg
    # >>> mock_something = AsyncMock(SpamEgg)
    from asynctest.mock import CoroutineMock as AsyncMock
else:
    from unittest import mock as async_mock
    from unittest.mock import AsyncMock
