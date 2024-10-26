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

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.eventbridge import EventBridgeHook
from airflow.providers.amazon.aws.operators.eventbridge import (
    EventBridgeDisableRuleOperator,
    EventBridgeEnableRuleOperator,
    EventBridgePutEventsOperator,
    EventBridgePutRuleOperator,
)

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

if TYPE_CHECKING:
    from unittest.mock import MagicMock

ENTRIES = [{"Detail": "test-detail", "Source": "test-source", "DetailType": "test-detail-type"}]
FAILED_ENTRIES_RESPONSE = [{"ErrorCode": "test_code"}, {"ErrorCode": "test_code"}]
EVENT_PATTERN = '{"source": ["aws.s3"]}'
RULE_NAME = "match_s3_events"


class TestEventBridgePutEventsOperator:
    def test_init(self):
        op = EventBridgePutEventsOperator(
            task_id="put_events_job",
            entries=ENTRIES,
            aws_conn_id="fake-conn-id",
            region_name="eu-central-1",
            verify="/spam/egg.pem",
            botocore_config={"read_timeout": 42},
        )
        assert op.entries == ENTRIES
        assert op.hook.client_type == "events"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "eu-central-1"
        assert op.hook._verify == "/spam/egg.pem"
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = EventBridgePutEventsOperator(task_id="put_events_job", entries=ENTRIES)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    @mock.patch.object(EventBridgeHook, "conn")
    def test_execute(self, mock_conn: MagicMock):
        hook_response = {"FailedEntryCount": 0, "Entries": [{"EventId": "foobar"}]}

        mock_conn.put_events.return_value = hook_response

        operator = EventBridgePutEventsOperator(
            task_id="put_events_job",
            entries=ENTRIES,
        )

        result = operator.execute(context={})

        assert result == ["foobar"]

    @mock.patch.object(EventBridgeHook, "conn")
    def test_failed_to_send(self, mock_conn: MagicMock):
        hook_response = {
            "FailedEntryCount": 1,
            "Entries": FAILED_ENTRIES_RESPONSE,
        }

        mock_conn.put_events.return_value = hook_response

        operator = EventBridgePutEventsOperator(
            task_id="failed_put_events_job",
            entries=ENTRIES,
        )

        with pytest.raises(AirflowException):
            operator.execute(context={})

    def test_template_fields(self):
        operator = EventBridgePutEventsOperator(
            task_id="failed_put_events_job",
            entries=ENTRIES,
        )
        validate_template_fields(operator)


class TestEventBridgePutRuleOperator:
    def test_init(self):
        op = EventBridgePutRuleOperator(
            task_id="events_put_rule_job",
            name=RULE_NAME,
            event_pattern=EVENT_PATTERN,
            aws_conn_id="fake-conn-id",
            region_name="eu-west-1",
            verify="/spam/egg.pem",
            botocore_config={"read_timeout": 42},
        )
        assert op.event_pattern == EVENT_PATTERN
        assert op.hook.client_type == "events"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify == "/spam/egg.pem"
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = EventBridgePutRuleOperator(
            task_id="events_put_rule_job", name=RULE_NAME, event_pattern=EVENT_PATTERN
        )
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    @mock.patch.object(EventBridgeHook, "conn")
    def test_execute(self, mock_conn: MagicMock):
        hook_response = {"RuleArn": "arn:aws:events:us-east-1:123456789012:rule/test"}
        mock_conn.put_rule.return_value = hook_response

        operator = EventBridgePutRuleOperator(
            task_id="events_put_rule_job",
            name=RULE_NAME,
            event_pattern=EVENT_PATTERN,
        )

        result = operator.execute(context={})

        assert result == hook_response

    def test_put_rule_with_bad_json_fails(self):
        operator = EventBridgePutRuleOperator(
            task_id="failed_put_rule_job",
            name=RULE_NAME,
            event_pattern="invalid json",
        )

        with pytest.raises(ValueError):
            operator.execute(None)

    def test_template_fields(self):
        operator = EventBridgePutRuleOperator(
            task_id="events_put_rule_job", name=RULE_NAME, event_pattern=EVENT_PATTERN
        )
        validate_template_fields(operator)


class TestEventBridgeEnableRuleOperator:
    def test_init(self):
        op = EventBridgeEnableRuleOperator(
            task_id="enable_rule_task",
            name=RULE_NAME,
            aws_conn_id="fake-conn-id",
            region_name="us-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.name == RULE_NAME
        assert op.hook.client_type == "events"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "us-west-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = EventBridgeEnableRuleOperator(task_id="enable_rule_task", name=RULE_NAME)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    @mock.patch.object(EventBridgeHook, "conn")
    def test_enable_rule(self, mock_conn: MagicMock):
        enable_rule = EventBridgeEnableRuleOperator(
            task_id="events_enable_rule_job",
            name=RULE_NAME,
        )

        enable_rule.execute(context={})
        mock_conn.enable_rule.assert_called_with(Name=RULE_NAME)

    def test_template_fields(self):
        operator = EventBridgeEnableRuleOperator(
            task_id="events_enable_rule_job",
            name=RULE_NAME,
        )
        validate_template_fields(operator)


class TestEventBridgeDisableRuleOperator:
    def test_init(self):
        op = EventBridgeDisableRuleOperator(
            task_id="disable_rule_task",
            name=RULE_NAME,
            aws_conn_id="fake-conn-id",
            region_name="ca-west-1",
            verify=True,
            botocore_config={"read_timeout": 42},
        )
        assert op.name == RULE_NAME
        assert op.hook.client_type == "events"
        assert op.hook.resource_type is None
        assert op.hook.aws_conn_id == "fake-conn-id"
        assert op.hook._region_name == "ca-west-1"
        assert op.hook._verify is True
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

        op = EventBridgeDisableRuleOperator(task_id="disable_rule_task", name=RULE_NAME)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

    @mock.patch.object(EventBridgeHook, "conn")
    def test_disable_rule(self, mock_conn: MagicMock):
        disable_rule = EventBridgeDisableRuleOperator(
            task_id="events_disable_rule_job",
            name=RULE_NAME,
        )

        disable_rule.execute(context={})
        mock_conn.disable_rule.assert_called_with(Name=RULE_NAME)

    def test_template_fields(self):
        operator = EventBridgeDisableRuleOperator(
            task_id="events_disable_rule_job",
            name=RULE_NAME,
        )
        validate_template_fields(operator)
