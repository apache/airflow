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
#

import unittest


class TestBaseHook(unittest.TestCase):
    """
    Tests for the BaseHook class.

    Note most concrete behaviours are tested in the LocalFsHook or the S3FsHook
    tests, as these have (mock) file systems to test against.
    """

    def test_for_connection(self):
        # TODO: Add test for the for_connection method.
        pass


if __name__ == '__main__':
    unittest.main()
