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

import glob
import mmap
import os
import unittest

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)
)
CORE_EXAMPLE_DAGS_FOLDER = os.path.join(ROOT_FOLDER, "airflow", "example_dags")


class TestExampleDags(unittest.TestCase):
    def test_reference_to_providers_is_illegal(self):
        for filename in glob.glob(f"{CORE_EXAMPLE_DAGS_FOLDER}/**.py"):
            self.assert_file_not_contains(filename, "providers")

    def assert_file_not_contains(self, filename, pattern):
        with open(filename, 'rb', 0) as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as content:
            if content.find(bytes(pattern, 'utf-8')) != -1:
                self.fail(f"File {filename} contians illegal pattern - {pattern}")
