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
from parameterized import parameterized

from tests.test_utils.system_tests_class import SystemTest


@SystemTest.skip()
class TestExampleDagsSystem(SystemTest):
    @parameterized.expand([
        "example_bash_operator",
        "example_branch_operator"
    ])
    def test_dag_example(self, dag_id):
        self.run_dag(dag_id=dag_id)
