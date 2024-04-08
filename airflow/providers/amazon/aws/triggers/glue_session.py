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

from typing import Any, AsyncIterator

from airflow.providers.amazon.aws.hooks.glue_session import GlueSessionHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class GlueSessionReadyTrigger(BaseTrigger):
    """
    Watches for a glue session, triggers when it is ready.

    :param session_id: glue session id
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        session_id: str,
        aws_conn_id: str | None,
        session_poll_interval: int | float,
    ):
        super().__init__()
        self.session_id = session_id
        self.aws_conn_id = aws_conn_id
        self.session_poll_interval = session_poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            # dynamically generate the fully qualified name of the class
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "session_id": self.session_id,
                "aws_conn_id": self.aws_conn_id,
                "session_poll_interval": self.session_poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = GlueSessionHook(aws_conn_id=self.aws_conn_id, session_poll_interval=self.session_poll_interval)
        await hook.async_session_readiness(self.session_id)
        yield TriggerEvent({"status": "ready", "message": "Session ready", "value": self.session_id})
