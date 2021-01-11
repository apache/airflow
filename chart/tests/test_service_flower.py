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
from parameterized import parameterized

from tests.helm_template_generator import render_chart

CELERY_EXECUTORS_PARAMS = [("CeleryExecutor",), ("CeleryKubernetesExecutor",)]


class ServiceFlowerTest(unittest.TestCase):
    @parameterized.expand(CELERY_EXECUTORS_PARAMS)
    def test_should_pass_validation_with_celery_executor(self, executor):
        render_chart(
            values={"executor": executor},
            show_only=["templates/flower/flower-service.yaml"],
        )  # checks that no validation exception is raised

    @parameterized.expand(CELERY_EXECUTORS_PARAMS)
    def test_should_allow_more_than_one_annotation(self, executor):
        docs = render_chart(
            values={"executor": executor, "flower": {"service": {"annotations": {"aa": "bb", "cc": "dd"}}}},
            show_only=["templates/flower/flower-service.yaml"],
        )
        self.assertEqual({"aa": "bb", "cc": "dd"}, jmespath.search("metadata.annotations", docs[0]))
