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

import json

import pytest

from airflow.configuration import conf
from airflow.providers.amazon.aws.executors.batch.batch_executor_config import build_submit_kwargs
from airflow.providers.amazon.aws.executors.batch.utils import CONFIG_GROUP_NAME, AllBatchConfigKeys

from tests_common.test_utils.config import conf_vars


class TestBuildSubmitKwargs:
    @conf_vars(
        {
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_DEFINITION): "job-definition",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_NAME): "job-name",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_QUEUE): "job-queue",
        }
    )
    def test_builds_submit_kwargs_from_config(self):
        assert build_submit_kwargs(conf) == {
            "containerOverrides": {"command": []},
            "jobDefinition": "job-definition",
            "jobName": "job-name",
            "jobQueue": "job-queue",
        }

    @conf_vars(
        {
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_DEFINITION): "job-definition",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_NAME): "job-name",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.JOB_QUEUE): "job-queue",
            (CONFIG_GROUP_NAME, AllBatchConfigKeys.SUBMIT_JOB_KWARGS): json.dumps(
                {
                    "containerOverrides": {"environment": [{"name": "KEY", "value": "value"}]},
                    "retryStrategy": {"attempts": 2},
                    "tags": {"custom_key": "custom_value"},
                }
            ),
        }
    )
    def test_templated_kwargs_extend_config_values(self):
        assert build_submit_kwargs(conf) == {
            "containerOverrides": {
                "command": [],
                "environment": [{"name": "KEY", "value": "value"}],
            },
            "jobDefinition": "job-definition",
            "jobName": "job-name",
            "jobQueue": "job-queue",
            "retryStrategy": {"attempts": 2},
            "tags": {"custom_key": "custom_value"},
        }

    @pytest.mark.parametrize(
        ("submit_job_kwargs", "match"),
        [
            ({"nodeOverrides": {}}, "Multi-node jobs are not currently supported."),
            ({"eksPropertiesOverride": {}}, "Eks jobs are not currently supported."),
        ],
    )
    def test_rejects_unsupported_submit_job_kwargs(self, submit_job_kwargs, match):
        with conf_vars(
            {
                (CONFIG_GROUP_NAME, AllBatchConfigKeys.SUBMIT_JOB_KWARGS): json.dumps(submit_job_kwargs),
            }
        ):
            with pytest.raises(KeyError, match=match):
                build_submit_kwargs(conf)
