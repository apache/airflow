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
import os

import pytest

from airflow.providers.amazon.aws.executors.batch import batch_executor_config
from airflow.providers.amazon.aws.executors.batch.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    AllBatchConfigKeys,
)
from airflow.providers.common.compat.sdk import conf
from airflow.utils.helpers import convert_camel_to_snake


class TestBatchExecutorConfig:
    @staticmethod
    def _unset_conf():
        for env in os.environ:
            if env.startswith(f"AIRFLOW__{CONFIG_GROUP_NAME.upper()}__"):
                os.environ.pop(env)

    def teardown_method(self) -> None:
        self._unset_conf()

    def test_config_defaults_are_applied(self):
        submit_kwargs = batch_executor_config.build_submit_kwargs(conf)
        found_keys = {convert_camel_to_snake(key): key for key in submit_kwargs.keys()}

        for expected_key, expected_value in CONFIG_DEFAULTS.items():
            # these config options are used by the executor,
            # but are not expected to appear in the submit_kwargs.
            if expected_key in [
                AllBatchConfigKeys.AWS_CONN_ID,
                AllBatchConfigKeys.CHECK_HEALTH_ON_STARTUP,
                AllBatchConfigKeys.MAX_SUBMIT_JOB_ATTEMPTS,
                AllBatchConfigKeys.SUBMIT_JOB_KWARGS,
                AllBatchConfigKeys.REGION_NAME,
            ]:
                assert expected_key not in found_keys.keys()
            else:
                assert expected_key in found_keys.keys()
                assert expected_value == submit_kwargs[found_keys[expected_key]]

    def test_verify_tags_are_used_as_provided(self):
        """Confirm that the ``tags`` provided are not converted to camelCase."""
        templated_tags = {"Apache": "Airflow"}

        provided_run_submit_kwargs = {
            "tags": templated_tags,  # The user should be allowed to pass arbitrary submit_job args
        }

        run_submit_kwargs_env_key = (
            f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllBatchConfigKeys.SUBMIT_JOB_KWARGS}".upper()
        )
        os.environ[run_submit_kwargs_env_key] = json.dumps(provided_run_submit_kwargs)
        submit_kwargs = batch_executor_config.build_submit_kwargs(conf)

        # Verify that tag names are exempt from the camel-case conversion.
        assert submit_kwargs["tags"] == templated_tags

    @pytest.mark.parametrize(
        ("submit_job_kwargs", "match"),
        [
            ({"nodeOverrides": {}}, "Multi-node jobs are not currently supported."),
            ({"eksPropertiesOverride": {}}, "Eks jobs are not currently supported."),
        ],
    )
    def test_rejects_unsupported_submit_job_kwargs(self, submit_job_kwargs, match):
        submit_job_kwargs_env_key = (
            f"AIRFLOW__{CONFIG_GROUP_NAME}__{AllBatchConfigKeys.SUBMIT_JOB_KWARGS}".upper()
        )
        os.environ[submit_job_kwargs_env_key] = json.dumps(submit_job_kwargs)

        with pytest.raises(KeyError, match=match):
            batch_executor_config.build_submit_kwargs(conf)
