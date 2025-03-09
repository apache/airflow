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

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.glue import GlueDataQualityHook
from airflow.providers.amazon.aws.sensors.glue import (
    GlueDataQualityRuleRecommendationRunSensor,
    GlueDataQualityRuleSetEvaluationRunSensor,
)

SAMPLE_RESPONSE_GET_DATA_QUALITY_EVALUATION_RUN_SUCCEEDED = {
    "RunId": "12345",
    "Status": "SUCCEEDED",
    "ResultIds": ["dqresult-123456"],
}

SAMPLE_RESPONSE_GET_DATA_QUALITY_EVALUATION_RUN_RUNNING = {
    "RunId": "12345",
    "Status": "RUNNING",
    "ResultIds": ["dqresult-123456"],
}

SAMPLE_RESPONSE_GET_DATA_QUALITY_RESULT = {
    "RunId": "12345",
    "ResultIds": ["dqresult-123456"],
    "Results": [
        {
            "ResultId": "dqresult-123456",
            "RulesetName": "rulesetOne",
            "RuleResults": [
                {
                    "Name": "Rule_1",
                    "Description": "RowCount between 150000 and 600000",
                    "EvaluatedMetrics": {"Dataset.*.RowCount": 300000.0},
                    "Result": "PASS",
                },
                {
                    "Name": "Rule_2",
                    "Description": "ColumnLength 'marketplace' between 1 and 2",
                    "EvaluationMessage": "Value: 9.0 does not meet the constraint requirement!",
                    "Result": "FAIL",
                    "EvaluatedMetrics": {
                        "Column.marketplace.MaximumLength": 9.0,
                        "Column.marketplace.MinimumLength": 2.0,
                    },
                },
            ],
        }
    ],
}

RULES = """
        Rules = [
                RowCount between 2 and 8,
                IsComplete "name"
             ]
        """

SAMPLE_RESPONSE_GET_DATA_RULE_RECOMMENDATION_RUN_SUCCEEDED = {
    "RunId": "12345",
    "Status": "SUCCEEDED",
    "DataSource": {"GlueTable": {"DatabaseName": "TestDB", "TableName": "TestTable"}},
    "RecommendedRuleset": RULES,
}

SAMPLE_RESPONSE_DATA_RULE_RECOMMENDATION_RUN_RUNNING = {"RunId": "12345", "Status": "RUNNING"}


class TestGlueDataQualityRuleSetEvaluationRunSensor:
    SENSOR = GlueDataQualityRuleSetEvaluationRunSensor

    def setup_method(self):
        self.default_args = dict(
            task_id="test_data_quality_ruleset_evaluation_run_sensor",
            evaluation_run_id="12345",
            poke_interval=5,
            max_retries=0,
        )
        self.sensor = self.SENSOR(**self.default_args, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = self.SENSOR(**self.default_args)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.SENSOR(
            **self.default_args,
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

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_poke_success_state(self, mock_conn):
        mock_conn.get_data_quality_ruleset_evaluation_run.return_value = (
            SAMPLE_RESPONSE_GET_DATA_QUALITY_EVALUATION_RUN_SUCCEEDED
        )

        assert self.sensor.poke({}) is True

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_poke_intermediate_state(self, mock_conn):
        mock_conn.get_data_quality_ruleset_evaluation_run.return_value = (
            SAMPLE_RESPONSE_GET_DATA_QUALITY_EVALUATION_RUN_RUNNING
        )

        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_poke_failure_states(self, mock_conn, state):
        mock_conn.get_data_quality_ruleset_evaluation_run.return_value = {
            "RunId": "12345",
            "Status": state,
            "ResultIds": ["dqresult-123456"],
            "ErrorString": "unknown error",
        }

        sensor = self.SENSOR(**self.default_args, aws_conn_id=None)

        message = f"Error: AWS Glue data quality ruleset evaluation run RunId: 12345 Run Status: {state}: unknown error"

        with pytest.raises(AirflowException, match=message):
            sensor.poke({})

        mock_conn.get_data_quality_ruleset_evaluation_run.assert_called_once_with(RunId="12345")

    def test_sensor_defer(self):
        """Test the execute method raise TaskDeferred if running sensor in deferrable mode"""
        sensor = GlueDataQualityRuleSetEvaluationRunSensor(
            task_id="test_task",
            poke_interval=0,
            evaluation_run_id="12345",
            aws_conn_id="aws_default",
            deferrable=True,
        )

        with pytest.raises(TaskDeferred):
            sensor.execute(context=None)

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_execute_complete_succeeds_if_status_in_succeeded_states(self, mock_conn, caplog):
        mock_conn.get_evaluation_run_results.return_value = SAMPLE_RESPONSE_GET_DATA_QUALITY_RESULT

        op = GlueDataQualityRuleSetEvaluationRunSensor(
            task_id="test_data_quality_ruleset_evaluation_run_sensor",
            evaluation_run_id="12345",
            poke_interval=0,
            aws_conn_id="aws_default",
            deferrable=True,
        )
        event = {"status": "success", "evaluation_run_id": "12345"}
        op.execute_complete(context={}, event=event)

        assert "AWS Glue data quality ruleset evaluation run completed." in caplog.messages

    def test_execute_complete_fails_if_status_in_failure_states(self):
        op = GlueDataQualityRuleSetEvaluationRunSensor(
            task_id="test_data_quality_ruleset_evaluation_run_sensor",
            evaluation_run_id="12345",
            poke_interval=0,
            aws_conn_id="aws_default",
            deferrable=True,
        )
        event = {"status": "failure"}
        with pytest.raises(AirflowException):
            op.execute_complete(context={}, event=event)


class TestGlueDataQualityRuleRecommendationRunSensor:
    SENSOR = GlueDataQualityRuleRecommendationRunSensor

    def setup_method(self):
        self.default_args = dict(
            task_id="test_data_quality_rule_recommendation_sensor",
            recommendation_run_id="12345",
            poke_interval=5,
            max_retries=0,
        )
        self.sensor = self.SENSOR(**self.default_args, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = self.SENSOR(**self.default_args)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.SENSOR(
            **self.default_args,
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

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_poke_success_state(self, mock_conn):
        mock_conn.get_data_quality_rule_recommendation_run.return_value = (
            SAMPLE_RESPONSE_GET_DATA_RULE_RECOMMENDATION_RUN_SUCCEEDED
        )
        assert self.sensor.poke({}) is True

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_poke_intermediate_state(self, mock_conn):
        mock_conn.get_data_quality_rule_recommendation_run.return_value = (
            SAMPLE_RESPONSE_DATA_RULE_RECOMMENDATION_RUN_RUNNING
        )
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_poke_failure_states(self, mock_conn, state):
        mock_conn.get_data_quality_rule_recommendation_run.return_value = {
            "RunId": "12345",
            "Status": state,
            "ErrorString": "unknown error",
        }

        sensor = self.SENSOR(**self.default_args, aws_conn_id=None)

        message = (
            f"Error: AWS Glue data quality recommendation run RunId: 12345 Run Status: {state}: unknown error"
        )

        with pytest.raises(AirflowException, match=message):
            sensor.poke({})

        mock_conn.get_data_quality_rule_recommendation_run.assert_called_once_with(RunId="12345")

    def test_sensor_defer(self):
        """Test the execute method raise TaskDeferred if running sensor in deferrable mode"""
        sensor = GlueDataQualityRuleRecommendationRunSensor(
            task_id="test_task",
            poke_interval=0,
            recommendation_run_id="12345",
            aws_conn_id="aws_default",
            deferrable=True,
        )

        with pytest.raises(TaskDeferred):
            sensor.execute(context=None)

    @mock.patch.object(GlueDataQualityHook, "conn")
    def test_execute_complete_succeeds_if_status_in_succeeded_states(self, mock_conn, caplog):
        mock_conn.get_data_quality_rule_recommendation_run.return_value = (
            SAMPLE_RESPONSE_GET_DATA_RULE_RECOMMENDATION_RUN_SUCCEEDED
        )

        op = GlueDataQualityRuleRecommendationRunSensor(
            task_id="test_data_quality_rule_recommendation_run_sensor",
            recommendation_run_id="12345",
            poke_interval=0,
            aws_conn_id="aws_default",
            deferrable=True,
        )
        event = {"status": "success", "recommendation_run_id": "12345"}
        op.execute_complete(context={}, event=event)
        assert "AWS Glue data quality recommendation run completed." in caplog.messages

    def test_execute_complete_fails_if_status_in_failure_states(self):
        op = GlueDataQualityRuleRecommendationRunSensor(
            task_id="test_data_quality_rule_recommendation_run_sensor",
            recommendation_run_id="12345",
            poke_interval=0,
            aws_conn_id="aws_default",
            deferrable=True,
        )
        event = {"status": "failure"}

        with pytest.raises(AirflowException):
            op.execute_complete(context={}, event=event)
