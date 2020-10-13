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

from airflow.upgrade.rules.pod_template_file_rule import PodTemplateFileRule
from tests.test_utils.config import conf_vars


class TestPodTemplateFileRule(unittest.TestCase):
    @conf_vars({
        ("core", "executor"): "KubernetesExecutor",
        ("kubernetes", "pod_template_file"): None,
    })
    def test_should_return_error(self):
        rule = PodTemplateFileRule()
        result = rule.check()
        self.assertEqual(
            result,
            "Please create a pod_template_file by running `airflow generate_pod_template`.\n"
            "This will generate a pod using your aiflow.cfg settings"
        )
