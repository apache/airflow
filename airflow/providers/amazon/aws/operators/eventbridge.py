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

from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.eventbridge import EventBridgeHook
from airflow.utils.helpers import prune_dict

if TYPE_CHECKING:
    from airflow.utils.context import Context


class EventBridgePutEventsOperator(BaseOperator):
    """
    Put Events onto Amazon EventBridge.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EventBridgePutEventsOperator`

    :param entries: the list of events to be put onto EventBridge, each event is a dict (required)
    :param endpoint_id: the URL subdomain of the endpoint
    :param aws_conn_id: the AWS connection to use
    :param region_name: the region where events are to be sent

    """

    template_fields: Sequence[str] = ("entries", "endpoint_id", "aws_conn_id", "region_name")

    def __init__(
        self,
        *,
        entries: list[dict],
        endpoint_id: str | None = None,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.entries = entries
        self.endpoint_id = endpoint_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    @cached_property
    def hook(self) -> EventBridgeHook:
        """Create and return an EventBridgeHook."""
        return EventBridgeHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    def execute(self, context: Context):

        response = self.hook.conn.put_events(
            **prune_dict(
                {
                    "Entries": self.entries,
                    "EndpointId": self.endpoint_id,
                }
            )
        )

        self.log.info("Sent %d events to EventBridge.", len(self.entries))

        if response.get("FailedEntryCount"):
            for event in response["Entries"]:
                if "ErrorCode" in event:
                    self.log.error(event)

            raise AirflowException(
                f"{response['FailedEntryCount']} entries in this request have failed to send."
            )

        if self.do_xcom_push:
            return [e["EventId"] for e in response["Entries"]]


class EventBridgePutRuleOperator(BaseOperator):
    """
    Create or update a specified EventBridge rule.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EventBridgePutRuleOperator`

    :param name: name of the rule to create or update (required)
    :param description: description of the rule
    :param event_bus_name: name or ARN of the event bus to associate with this rule
    :param event_pattern: pattern of events to be matched to this rule
    :param role_arn: the Amazon Resource Name of the IAM role associated with the rule
    :param schedule_expression: the scheduling expression (for example, a cron or rate expression)
    :param state: indicates whether rule is set to be "ENABLED" or "DISABLED"
    :param tags: list of key-value pairs to associate with the rule
    :param region: the region where rule is to be created or updated

    """

    template_fields: Sequence[str] = (
        "aws_conn_id",
        "name",
        "description",
        "event_bus_name",
        "event_pattern",
        "role_arn",
        "schedule_expression",
        "state",
        "tags",
        "region_name",
    )

    def __init__(
        self,
        *,
        name: str,
        description: str | None = None,
        event_bus_name: str | None = None,
        event_pattern: str | None = None,
        role_arn: str | None = None,
        schedule_expression: str | None = None,
        state: str | None = None,
        tags: list | None = None,
        region_name: str | None = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.name = name
        self.description = description
        self.event_bus_name = event_bus_name
        self.event_pattern = event_pattern
        self.role_arn = role_arn
        self.region_name = region_name
        self.schedule_expression = schedule_expression
        self.state = state
        self.tags = tags
        self.aws_conn_id = aws_conn_id

    @cached_property
    def hook(self) -> EventBridgeHook:
        """Create and return an EventBridgeHook."""
        return EventBridgeHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    def execute(self, context: Context):

        self.log.info('Sending rule "%s" to EventBridge.', self.name)

        return self.hook.put_rule(
            name=self.name,
            description=self.description,
            event_bus_name=self.event_bus_name,
            event_pattern=self.event_pattern,
            role_arn=self.role_arn,
            schedule_expression=self.schedule_expression,
            state=self.state,
            tags=self.tags,
        )


class EventBridgeEnableRuleOperator(BaseOperator):
    """
    Enable an EventBridge Rule.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EventBridgeEnableRuleOperator`

    :param name: the name of the rule to enable
    :param event_bus_name: the name or ARN of the event bus associated with the rule (default if omitted)
    :param aws_conn_id: the AWS connection to use
    :param region_name: the region of the rule to be enabled

    """

    template_fields: Sequence[str] = ("name", "event_bus_name", "region_name", "aws_conn_id")

    def __init__(
        self,
        *,
        name: str,
        event_bus_name: str | None = None,
        region_name: str | None = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.name = name
        self.event_bus_name = event_bus_name
        self.region_name = region_name
        self.aws_conn_id = aws_conn_id

    @cached_property
    def hook(self) -> EventBridgeHook:
        """Create and return an EventBridgeHook."""
        return EventBridgeHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    def execute(self, context: Context):
        self.hook.conn.enable_rule(
            **prune_dict(
                {
                    "Name": self.name,
                    "EventBusName": self.event_bus_name,
                }
            )
        )

        self.log.info('Enabled rule "%s"', self.name)


class EventBridgeDisableRuleOperator(BaseOperator):
    """
    Disable an EventBridge Rule.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EventBridgeDisableRuleOperator`

    :param name: the name of the rule to disable
    :param event_bus_name: the name or ARN of the event bus associated with the rule (default if omitted)
    :param aws_conn_id: the AWS connection to use
    :param region_name: the region of the rule to be disabled

    """

    template_fields: Sequence[str] = ("name", "event_bus_name", "region_name", "aws_conn_id")

    def __init__(
        self,
        *,
        name: str,
        event_bus_name: str | None = None,
        region_name: str | None = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.name = name
        self.event_bus_name = event_bus_name
        self.region_name = region_name
        self.aws_conn_id = aws_conn_id

    @cached_property
    def hook(self) -> EventBridgeHook:
        """Create and return an EventBridgeHook."""
        return EventBridgeHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    def execute(self, context: Context):

        self.hook.conn.disable_rule(
            **prune_dict(
                {
                    "Name": self.name,
                    "EventBusName": self.event_bus_name,
                }
            )
        )

        self.log.info('Disabled rule "%s"', self.name)
