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
from airflow.providers.amazon.aws.utils import trim_none_values

if TYPE_CHECKING:
    from airflow.utils.context import Context


class EventBridgePutEventsOperator(BaseOperator):
    """
    Put Events onto Amazon EventBridge.

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
            **trim_none_values(
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
