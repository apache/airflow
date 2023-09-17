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

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.helpers import prune_dict


def _validate_json(pattern: str) -> None:
    try:
        json.loads(pattern)
    except ValueError:
        raise ValueError("`event_pattern` must be a valid JSON string.")


class EventBridgeHook(AwsBaseHook):
    """Amazon EventBridge Hook."""

    def __init__(self, *args, **kwargs):
        super().__init__(client_type="events", *args, **kwargs)

    def put_rule(
        self,
        name: str,
        description: str | None = None,
        event_bus_name: str | None = None,
        event_pattern: str | None = None,
        role_arn: str | None = None,
        schedule_expression: str | None = None,
        state: str | None = None,
        tags: list[dict] | None = None,
        **kwargs,
    ):
        """
        Create or update an EventBridge rule.

        :param name: name of the rule to create or update (required)
        :param description: description of the rule
        :param event_bus_name: name or ARN of the event bus to associate with this rule
        :param event_pattern: pattern of events to be matched to this rule
        :param role_arn: the Amazon Resource Name of the IAM role associated with the rule
        :param schedule_expression: the scheduling expression (for example, a cron or rate expression)
        :param state: indicates whether rule is set to be "ENABLED" or "DISABLED"
        :param tags: list of key-value pairs to associate with the rule

        """
        if not (event_pattern or schedule_expression):
            raise ValueError(
                "One of `event_pattern` or `schedule_expression` are required in order to "
                "put or update your rule."
            )

        if state and state not in ["ENABLED", "DISABLED"]:
            raise ValueError("`state` must be specified as ENABLED or DISABLED.")

        if event_pattern:
            _validate_json(event_pattern)

        put_rule_kwargs: dict[str, str | list] = {
            **prune_dict(
                {
                    "Name": name,
                    "Description": description,
                    "EventBusName": event_bus_name,
                    "EventPattern": event_pattern,
                    "RoleArn": role_arn,
                    "ScheduleExpression": schedule_expression,
                    "State": state,
                    "Tags": tags,
                }
            )
        }

        return self.conn.put_rule(**put_rule_kwargs)
