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

import pytest

from airflow.providers.amazon.aws.triggers.glue_databrew import (
    GlueDataBrewJobCompleteTrigger,
)

TEST_JOB_NAME = "test_job_name"
TEST_JOB_RUN_ID = "a1234"
TEST_JOB_RUN_STATUS = "SUCCEEDED"


@pytest.fixture
def trigger():
    return GlueDataBrewJobCompleteTrigger(
        aws_conn_id="aws_default", job_name=TEST_JOB_NAME, run_id=TEST_JOB_RUN_ID
    )


class TestGlueDataBrewJobCompleteTrigger:
    def test_serialize(self, trigger):
        class_path, args = trigger.serialize()

        class_name = class_path.split(".")[-1]
        clazz = globals()[class_name]
        instance = clazz(**args)

        class_path2, args2 = instance.serialize()

        assert class_path == class_path2
        assert args == args2
