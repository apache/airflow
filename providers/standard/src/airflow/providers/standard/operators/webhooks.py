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
"""
Operators that wait for a delivery to an inbound webhook.

.. note:: |experimental|
"""

from __future__ import annotations

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.standard.version_compat import BaseOperator

# Detect the feature, not the version: wait_for_webhook first ships in Airflow 3.5, and a source build
# of main can still report the previous version.
if not hasattr(BaseOperator, "wait_for_webhook"):
    raise AirflowOptionalProviderFeatureException("Webhook waits need Airflow 3.5+.")

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any, NoReturn

from airflow.providers.standard.exceptions import WebhookWaitTimeoutError

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class WaitForWebhookOperator(BaseOperator):
    """
    Wait for a delivery to an inbound webhook and return it.

    The task registers a wait with the API server and parks in the ``awaiting_input`` state, which holds
    no worker slot and no trigger. The first delivery to ``webhook`` whose event is one of ``events`` and
    whose attributes match ``match`` resumes the task, which returns the delivery as its XCom:
    ``{"webhook", "event", "delivery_id", "received_at", "attributes", "payload"}``. The payload is what
    the webhook's ``store_payload`` keeps of the delivery, so anyone who can read the task's XCom can
    read it.

    Within a webhook, only one running or parked task instance can wait with the same ``events`` and
    ``match``; a second one fails when it registers its wait.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:WaitForWebhookOperator`

    .. note:: |experimental|

    :param webhook: The webhook's name, as listed in ``[webhooks] names``. (templated)
    :param events: The delivery events to accept, or None to accept any event. (templated)
    :param match: The attributes the delivery must have, each mapped to a value or a list of values of
        which one must match. Values compare as strings. XComArgs and Jinja templates in it are resolved
        when the task runs. (templated)
    :param timeout: How long to wait, at most 365 days, or None for the API server's
        ``[webhooks] default_wait_timeout``. The task's ``execution_timeout`` does not run while it
        waits.
    :raises WebhookWaitTimeoutError: if no delivery matches before the timeout.
    :raises WebhookWaitUnsupported: if the API server cannot hold webhook waits. The operator has no
        other way to wait, so the task fails.
    """

    template_fields: Sequence[str] = ("webhook", "events", "match")

    def __init__(
        self,
        *,
        webhook: str,
        events: Sequence[str | int] | None = None,
        match: Mapping[str, str | int | Sequence[str | int]] | None = None,
        timeout: timedelta | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.webhook = webhook
        self.events = events
        self.match = match
        self.timeout = timeout

    def execute(self, context: Context) -> NoReturn:
        self.wait_for_webhook(
            webhook=self.webhook,
            events=self.events,
            match=self.match,
            method_name="execute_complete",
            timeout=self.timeout,
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> dict[str, Any]:
        """Return the delivery that resumed the task, or raise if the wait timed out."""
        # A wait ends without a delivery only at its timeout, and only that event carries "error".
        if "error" in event:
            raise WebhookWaitTimeoutError(event["error"])
        return event
