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

import unittest

from parameterized import parameterized

from airflow.api_connexion.endpoints.version_endpoint import VersionInfo
from airflow.api_connexion.schemas.version_schema import version_info_schema


class TestVersionInfoSchema(unittest.TestCase):
    @parameterized.expand(
        [("GIT_COMMIT",), (None,),]
    )
    def test_serialize(self, git_commit):
        version_info = VersionInfo("VERSION", git_commit)
        current_data = version_info_schema.dump(version_info)

        expected_result = {'version': 'VERSION', 'git_version': git_commit}
        self.assertEqual(expected_result, current_data)
