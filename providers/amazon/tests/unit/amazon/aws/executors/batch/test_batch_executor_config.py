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
from unittest import mock

import pytest

from airflow.providers.amazon.aws.executors.batch.batch_executor_config import (
    _fetch_config_values,
    _fetch_templated_kwargs,
    build_submit_kwargs,
)
from airflow.providers.amazon.aws.executors.batch.utils import (
    CONFIG_GROUP_NAME,
    AllBatchConfigKeys,
    BatchSubmitJobKwargsConfigKeys,
)


class TestFetchTemplatedKwargs:
    def test_valid_json_string(self):
        conf = mock.MagicMock()
        payload = {"paramA": "valA", "paramB": 123}
        conf.get.return_value = json.dumps(payload)

        result = _fetch_templated_kwargs(conf)

        conf.get.assert_called_once_with(CONFIG_GROUP_NAME, AllBatchConfigKeys.SUBMIT_JOB_KWARGS, fallback={})
        assert result == payload

    def test_fallback_empty_dict(self):
        conf = mock.MagicMock()
        conf.get.side_effect = lambda group, key, fallback: fallback

        result = _fetch_templated_kwargs(conf)
        assert result == {}


class TestFetchConfigValues:
    def test_prunes_none_values(self):
        conf = mock.MagicMock()
        test_values = {
            BatchSubmitJobKwargsConfigKeys.JOB_NAME: "test-job",
            BatchSubmitJobKwargsConfigKeys.JOB_QUEUE: "test-queue",
            BatchSubmitJobKwargsConfigKeys.JOB_DEFINITION: "test-def",
            BatchSubmitJobKwargsConfigKeys.EKS_PROPERTIES_OVERRIDE: None,
            BatchSubmitJobKwargsConfigKeys.NODE_OVERRIDE: None,
        }
        conf.get.side_effect = lambda group, key, fallback=None: test_values.get(key, fallback)

        result = _fetch_config_values(conf)

        assert result == {
            "job_name": "test-job",
            "job_queue": "test-queue",
            "job_definition": "test-def",
        }


class TestBuildSubmitKwargs:
    def test_basic_build_submit_kwargs(self):
        conf = mock.MagicMock()
        test_values = {
            BatchSubmitJobKwargsConfigKeys.JOB_NAME: "my-job",
            BatchSubmitJobKwargsConfigKeys.JOB_QUEUE: "my-queue",
            BatchSubmitJobKwargsConfigKeys.JOB_DEFINITION: "my-def",
        }
        conf.get.side_effect = lambda group, key, fallback=None: (
            "{}" if key == AllBatchConfigKeys.SUBMIT_JOB_KWARGS else test_values.get(key, fallback)
        )

        result = build_submit_kwargs(conf)

        assert result["jobName"] == "my-job"
        assert result["jobQueue"] == "my-queue"
        assert result["jobDefinition"] == "my-def"
        assert result["containerOverrides"] == {"command": []}

    def test_merges_templated_kwargs_and_preserves_container_overrides(self):
        conf = mock.MagicMock()
        templated = {
            "containerOverrides": {"environment": [{"name": "FOO", "value": "BAR"}]},
            "shareIdentifier": "share-123",
        }
        test_values = {
            BatchSubmitJobKwargsConfigKeys.JOB_NAME: "templated-job",
        }
        conf.get.side_effect = lambda group, key, fallback=None: (
            json.dumps(templated)
            if key == AllBatchConfigKeys.SUBMIT_JOB_KWARGS
            else test_values.get(key, fallback)
        )

        result = build_submit_kwargs(conf)

        assert result["jobName"] == "templated-job"
        assert result["shareIdentifier"] == "share-123"
        assert result["containerOverrides"]["command"] == []
        assert result["containerOverrides"]["environment"] == [{"name": "FOO", "value": "BAR"}]

    def test_rejects_node_overrides(self):
        conf = mock.MagicMock()
        templated = {"nodeOverrides": {"numNodes": 2}}
        conf.get.side_effect = lambda group, key, fallback=None: (
            json.dumps(templated) if key == AllBatchConfigKeys.SUBMIT_JOB_KWARGS else None
        )

        with pytest.raises(KeyError, match="Multi-node jobs are not currently supported."):
            build_submit_kwargs(conf)

    def test_rejects_eks_properties_override(self):
        conf = mock.MagicMock()
        templated = {"eksPropertiesOverride": {"podProperties": {}}}
        conf.get.side_effect = lambda group, key, fallback=None: (
            json.dumps(templated) if key == AllBatchConfigKeys.SUBMIT_JOB_KWARGS else None
        )

        with pytest.raises(KeyError, match="Eks jobs are not currently supported."):
            build_submit_kwargs(conf)

    def test_missing_command_in_container_overrides_raises_key_error(self):
        conf = mock.MagicMock()
        conf.get.side_effect = lambda group, key, fallback=None: (
            "{}" if key == AllBatchConfigKeys.SUBMIT_JOB_KWARGS else None
        )

        class DroppingCommandDict(dict):
            def __getitem__(self, key):
                val = super().__getitem__(key)
                if key == "containerOverrides" and isinstance(val, dict):
                    val.pop("command", None)
                return val

        with mock.patch(
            "airflow.providers.amazon.aws.executors.batch.batch_executor_config._fetch_config_values",
            return_value=DroppingCommandDict(),
        ):
            with pytest.raises(
                KeyError,
                match=r'SubmitJob API needs kwargs\["containerOverrides"\]\["command"\] field',
            ):
                build_submit_kwargs(conf)

    def test_json_decode_error_raises_value_error(self):
        conf = mock.MagicMock()
        conf.get.side_effect = lambda group, key, fallback=None: (
            "{}" if key == AllBatchConfigKeys.SUBMIT_JOB_KWARGS else None
        )

        with mock.patch(
            "airflow.providers.amazon.aws.executors.batch.batch_executor_config.json.dumps",
            return_value="not valid json",
        ):
            with pytest.raises(
                ValueError,
                match="AWS Batch Executor config values must be JSON serializable.",
            ):
                build_submit_kwargs(conf)
