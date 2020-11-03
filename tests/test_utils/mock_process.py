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

from typing import Optional
from unittest import mock


class MockDBConnection:
    def __init__(self, extra_dejson=None, *args, **kwargs):
        self.extra_dejson = extra_dejson
        self.get_records = mock.MagicMock(return_value=[['test_record']])

        output = kwargs.get('output', ['' for _ in range(10)])
        self.readline = mock.MagicMock(
            side_effect=[line.encode() for line in output])

    def status(self, *args, **kwargs):
        return True


class MockStdOut:
    def __init__(self, *args, **kwargs):
        output = kwargs.get('output', ['' for _ in range(10)])
        self.readline = mock.MagicMock(
            side_effect=[line.encode() for line in output])


class MockSubProcess:
    PIPE = -1
    STDOUT = -2
    returncode: Optional[int] = None

    def __init__(self, *args, **kwargs):
        self.stdout = MockStdOut(*args, **kwargs)

    def wait(self):
        return


class MockConnectionCursor:
    def __init__(self, *args, **kwargs):
        self.arraysize = None
        self.description = [('hive_server_hook.a', 'INT_TYPE', None, None, None, None, True),
                            ('hive_server_hook.b', 'INT_TYPE', None, None, None, None, True)]
        self.iterable = [(1, 1), (2, 2)]
        self.conn_exists = kwargs.get('exists', True)

    def close(self):
        pass

    def cursor(self):
        return self

    def execute(self, values=None):
        pass

    def exists(self, *args):
        return self.conn_exists

    def isfile(self, *args):
        return True

    def remove(self, *args):
        pass

    def upload(self, local_filepath, destination_filepath):
        pass

    def __next__(self):
        return self.iterable

    def __iter__(self):
        yield from self.iterable
