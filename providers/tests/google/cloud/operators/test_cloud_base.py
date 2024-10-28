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
from __future__ import annotations

import copy

from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry

from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

TASK_ID = "task-id"


class GoogleSampleOperator(GoogleCloudBaseOperator):
    def __init__(
        self,
        retry: Retry | _MethodDefault = DEFAULT,
        config: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.retry = retry
        self.config = config


class TestGoogleCloudBaseOperator:
    def test_handles_deepcopy_with_method_default(self):
        op = GoogleSampleOperator(task_id=TASK_ID)
        copied_op = copy.deepcopy(op)

        assert copied_op.retry == DEFAULT
        assert copied_op.config is None

    def test_handles_deepcopy_with_non_default_retry(self):
        op = GoogleSampleOperator(
            task_id=TASK_ID, retry=Retry(deadline=30), config={"config": "value"}
        )
        copied_op = copy.deepcopy(op)

        assert copied_op.retry.deadline == 30
        assert copied_op.config == {"config": "value"}
