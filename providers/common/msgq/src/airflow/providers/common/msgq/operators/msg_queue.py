#
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

import re
from collections.abc import Sequence
from functools import cached_property
from typing import Any, NoReturn

from airflow.exceptions import AirflowException, AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.common.msgq.hooks.msg_queue import MsgQueueHook

_PROVIDERS_MATCHER = re.compile(r"airflow\.providers\.(.*?)\.hooks.*")

_MIN_SUPPORTED_PROVIDERS_VERSION = {
    "amazon": "4.1.0",
    "apache.kafka": "2.1.0",
    "google": "8.2.0",
}

class BaseMsgQueueOperator(BaseOperator):
    """
    This is a base class for the generic Message Queue Operator to get a Queue Hook.

    The provided method is .get_queue_hook(). The default behavior will try to
    retrieve the Queue hook based on connection type.
    You can customize the behavior by overriding the .get_queue_hook() method.

    :param conn_id: reference to a specific message queue providers
    """

    conn_id_field = "conn_id"

    template_fields: Sequence[str] = ("conn_id", "message_queue", "hook_params")

    def __init__(
        self,
        *,
        conn_id: str | None = None,
        message_queue: str | None = None,
        hook_params: dict | None = None,
        retry_on_failure: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.message_queue = message_queue
        self.hook_params = hook_params or {}
        self.retry_on_failure = retry_on_failure


    @cached_property
    def _hook(self):
        """Get MsgQueue Hook based on connection type."""
        conn_id = getattr(self, self.conn_id_field)
        self.log.debug("Get connection for %s", conn_id)
        hook = self.get_hook(conn_id=conn_id, hook_params=self.hook_params)
        if not isinstance(hook, MsgQueueHook):
            raise AirflowException(
                f"You are trying to use `common-msgq` with {hook.__class__.__name__},"
                " but its provider does not support it. Please upgrade the provider to a version that"
                " supports `common-msgq`. The hook class should be a subclass of"
                " `airflow.providers.common.msgq.hooks.msq_queue.MsqQueueHook`."
                f" Got {hook.__class__.__name__} Hook with class hierarchy: {hook.__class__.mro()}"
            )

        if self.message_queue:
            if hook.conn_type == "kafka":
                hook.message_queue = self.message_queue
            else:
                hook.schema = self.message_queue

        return hook

   

    def _raise_exception(self, exception_string: str) -> NoReturn:
        if self.retry_on_failure:
            raise AirflowException(exception_string)
        raise AirflowFailException(exception_string)

class MsqQueuePublishOperator(BaseMsgQueueOperator):
    """
    Publish something onto a message queue. 

    :param topic
    :param message
    """
    def publish(self, message, topic) -> None:
        # Publish the specified message, with the topic on the message queue

        return

