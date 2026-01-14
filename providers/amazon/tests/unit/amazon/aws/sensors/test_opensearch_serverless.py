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

from unittest import mock

import pytest

from airflow.providers.amazon.aws.hooks.opensearch_serverless import OpenSearchServerlessHook
from airflow.providers.amazon.aws.sensors.opensearch_serverless import (
    OpenSearchServerlessCollectionActiveSensor,
)
from airflow.providers.common.compat.sdk import AirflowException


class TestOpenSearchServerlessCollectionActiveSensor:
    def setup_method(self):
        self.default_op_kwargs = dict(
            task_id="test_sensor",
            collection_id="knowledge_base_id",
            poke_interval=5,
            max_retries=1,
        )
        self.sensor = OpenSearchServerlessCollectionActiveSensor(**self.default_op_kwargs, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = OpenSearchServerlessCollectionActiveSensor(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = OpenSearchServerlessCollectionActiveSensor(
            **self.default_op_kwargs,
            aws_conn_id="aws-test-custom-conn",
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.aws_conn_id == "aws-test-custom-conn"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

    @pytest.mark.parametrize(
        ("collection_name", "collection_id", "expected_pass"),
        [
            pytest.param("name", "id", False, id="both_provided_fails"),
            pytest.param("name", None, True, id="only_name_provided_passes"),
            pytest.param(None, "id", True, id="only_id_provided_passes"),
        ],
    )
    def test_name_and_id_combinations(self, collection_name, collection_id, expected_pass):
        call_args = {
            "task_id": "test_sensor",
            "collection_name": collection_name,
            "collection_id": collection_id,
            "poke_interval": 5,
            "max_retries": 1,
        }
        if expected_pass:
            op = OpenSearchServerlessCollectionActiveSensor(**call_args)
            assert op.collection_id == collection_id
            assert op.collection_name == collection_name
        if not expected_pass:
            with pytest.raises(
                AttributeError, match="Either collection_ids or collection_names must be provided, not both."
            ):
                OpenSearchServerlessCollectionActiveSensor(**call_args)

    @pytest.mark.parametrize("state", list(OpenSearchServerlessCollectionActiveSensor.SUCCESS_STATES))
    @mock.patch.object(OpenSearchServerlessHook, "conn")
    def test_poke_success_states(self, mock_conn, state):
        mock_conn.batch_get_collection.return_value = {"collectionDetails": [{"status": state}]}
        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize("state", list(OpenSearchServerlessCollectionActiveSensor.INTERMEDIATE_STATES))
    @mock.patch.object(OpenSearchServerlessHook, "conn")
    def test_poke_intermediate_states(self, mock_conn, state):
        mock_conn.batch_get_collection.return_value = {"collectionDetails": [{"status": state}]}
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize("state", list(OpenSearchServerlessCollectionActiveSensor.FAILURE_STATES))
    @mock.patch.object(OpenSearchServerlessHook, "conn")
    def test_poke_failure_states(self, mock_conn, state):
        mock_conn.batch_get_collection.return_value = {"collectionDetails": [{"status": state}]}
        sensor = OpenSearchServerlessCollectionActiveSensor(**self.default_op_kwargs, aws_conn_id=None)
        with pytest.raises(AirflowException, match=sensor.FAILURE_MESSAGE):
            sensor.poke({})
