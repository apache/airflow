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

import jmespath
from tests.helm_template_generator import render_chart


class MigrateDatabaseJobTest(unittest.TestCase):
    def test_should_run_by_default(self):
        docs = render_chart(
            values={},
            show_only=["templates/migrate-database-job.yaml"],
        )

        self.assertRegex(docs[0]["kind"], "Job")
        self.assertEqual(
            "run-airflow-migrations", jmespath.search("spec.template.spec.containers[0].name", docs[0])
        )
