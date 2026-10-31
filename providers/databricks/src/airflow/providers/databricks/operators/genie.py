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
"""Operators for asking Databricks Genie spaces questions as standalone tasks."""

from __future__ import annotations

import hashlib
import time
from collections.abc import Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from airflow.providers.common.compat.sdk import AirflowFailException, BaseOperator, ResumableJobMixin, conf
from airflow.providers.databricks.hooks.genie import (
    _PENDING_STATUSES,
    DatabricksGenieError,
    DatabricksGenieHook,
    _truncate_result,
)
from airflow.providers.databricks.triggers.genie import DatabricksGenieMessageTrigger

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context

GenieMessageRef = dict[str, str]

_NOT_FOUND = "NOT_FOUND"
_STALE = "STALE"


class DatabricksGenieOperator(ResumableJobMixin, BaseOperator):
    """
    Ask a Databricks Genie space one question and return a bounded, structured answer.

    The operator submits the question once, persists the Genie conversation and message IDs to task
    state before waiting, then polls (or defers) until the message finishes. A retry after a worker
    crash or timeout reconnects to the stored message instead of asking again.

    Genie has no idempotency key, so a submission whose outcome is unknown (a timeout, a connection
    error or a 5xx response) cannot be told apart from one that was never received. The operator fails
    without retrying in that case and the next try refuses to submit again; set ``conversation_id`` and
    ``message_id`` to attach to the message you found, or ``resubmit_unconfirmed=True`` to ask again.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DatabricksGenieOperator`

    :param space_id: Genie space ID. (templated)
    :param question: Question to ask. Required unless ``message_id`` is set. (templated)
    :param databricks_conn_id: Databricks connection. Defaults to ``databricks_default``. (templated)
    :param conversation_id: Existing conversation to ask the question in. The operator never deletes it.
        Defaults to ``None``, which starts a new conversation. An empty string means ``None``. (templated)
    :param message_id: Existing message to wait for instead of asking a question. Requires
        ``conversation_id``. An empty string means ``None``. (templated)
    :param polling_period_seconds: Seconds between status checks. Defaults to ``5``.
    :param timeout: Maximum seconds to wait for the answer in one try. Timing out does not cancel the
        message; the next try reconnects to it. Defaults to ``1800``.
    :param resubmit_unconfirmed: Ask the question again even though an earlier submission's outcome is
        unknown and may have created a message. Defaults to ``False``.
    :param databricks_retry_limit: Attempts for each Genie read. Submissions are never retried.
        Defaults to ``3``.
    :param databricks_retry_delay: Minimum seconds between read attempts. Defaults to ``1``.
    :param deferrable: Release the worker while waiting. Defaults to ``[operators] default_deferrable``,
        with a fallback of ``False``.
    :param durable: When ``True`` (the default), the Genie message IDs are persisted to task state
        before waiting so a retry reconnects instead of asking again. Requires Airflow 3.3+; on earlier
        versions every try asks the question again.
    """

    template_fields: Sequence[str] = (
        "space_id",
        "question",
        "databricks_conn_id",
        "conversation_id",
        "message_id",
    )
    ui_color = "#1CB1C2"
    external_id_key = "databricks_genie_message"

    def __init__(
        self,
        *,
        space_id: str,
        question: str | None = None,
        databricks_conn_id: str = "databricks_default",
        conversation_id: str | None = None,
        message_id: str | None = None,
        polling_period_seconds: float = 5,
        timeout: float = 1800,
        resubmit_unconfirmed: bool = False,
        databricks_retry_limit: int = 3,
        databricks_retry_delay: float = 1,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        durable: bool | None = None,
        **kwargs: Any,
    ) -> None:
        # Named here (not left to **kwargs) so default_args reaches it on every supported Airflow version.
        if durable is not None:
            kwargs["durable"] = durable
        super().__init__(**kwargs)
        if polling_period_seconds <= 0:
            raise ValueError("polling_period_seconds must be positive")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.space_id = space_id
        self.question = question
        self.databricks_conn_id = databricks_conn_id
        self.conversation_id = conversation_id
        self.message_id = message_id
        self.polling_period_seconds = polling_period_seconds
        self.timeout = timeout
        self.resubmit_unconfirmed = resubmit_unconfirmed
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.deferrable = deferrable
        self._result: dict[str, Any] | None = None

    @cached_property
    def hook(self) -> DatabricksGenieHook:
        return DatabricksGenieHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            caller=type(self).__name__,
            poll_interval=self.polling_period_seconds,
        )

    @property
    def _submission_key(self) -> str:
        return f"{self.external_id_key}_submission"

    def _fingerprint(self) -> str:
        """Identify the question so a retry never reconnects to an answer for a different one."""
        material = "\x1f".join((self.space_id, self.conversation_id or "", self.question or ""))
        return hashlib.sha256(material.encode("utf-8")).hexdigest()

    def _task_state_store(self, context: Context) -> Any:
        return context.get("task_state_store") if self.durable else None

    def _validate(self) -> None:
        self.conversation_id = self.conversation_id or None
        self.message_id = self.message_id or None
        self.hook._validate_space_id(self.space_id)
        if self.message_id is not None:
            if self.conversation_id is None:
                raise ValueError("message_id requires conversation_id")
        elif not isinstance(self.question, str) or not self.question.strip():
            raise ValueError("question must be a non-empty string unless message_id is set")

    def execute(self, context: Context) -> dict[str, Any]:
        self._validate()
        if self.message_id is None:
            return self.execute_resumable(context)
        ref = {
            "space_id": self.space_id,
            "conversation_id": self.conversation_id,
            "message_id": self.message_id,
            "fingerprint": self._fingerprint(),
        }
        self.log.info(
            "Attaching to Genie message %s in conversation %s", self.message_id, self.conversation_id
        )
        if (store := self._task_state_store(context)) is not None:
            store.set(self.external_id_key, ref)
            store.delete(self._submission_key)
        return self.poll_until_complete(ref, context)

    def submit_job(self, context: Context) -> Any:
        store = self._task_state_store(context)
        if store is not None:
            if store.get(self._submission_key) and not self.resubmit_unconfirmed:
                raise AirflowFailException(
                    "An earlier try submitted this Genie question but Databricks never confirmed the "
                    "outcome, so a message may already exist. Inspect the Genie space, then set "
                    "conversation_id and message_id to attach to it, or set resubmit_unconfirmed=True "
                    "to ask again."
                )
            store.set(self._submission_key, {"fingerprint": self._fingerprint()})

        space = quote(self.space_id, safe="")
        if self.conversation_id is None:
            endpoint = f"spaces/{space}/start-conversation"
        else:
            endpoint = f"spaces/{space}/conversations/{quote(self.conversation_id, safe='')}/messages"
        try:
            created = self.hook._request(
                "POST",
                endpoint,
                body={"content": self.question, "enable_visualization": False},
                retry_read=False,
            )
        except DatabricksGenieError as err:
            if err.status_code is not None and err.status_code < 500:
                # Databricks rejected the request, so nothing was created and a retry may submit again.
                if store is not None:
                    store.delete(self._submission_key)
                raise
            raise AirflowFailException(str(err)) from err

        conversation_id = created.get("conversation_id")
        message_id = created.get("message_id") or created.get("id")
        if not isinstance(conversation_id, str) or not isinstance(message_id, str):
            raise AirflowFailException(
                "Genie accepted the question but returned no conversation or message ID; "
                "the question will not be asked again."
            )
        ref = {
            "space_id": self.space_id,
            "conversation_id": conversation_id,
            "message_id": message_id,
            "fingerprint": self._fingerprint(),
        }
        self.log.info("Asked Genie message %s in conversation %s", message_id, conversation_id)
        if store is not None:
            # Persist before dropping the marker so no crash can lose both.
            store.set(self.external_id_key, ref)
            store.delete(self._submission_key)
        return ref

    def _message_path(self, ref: GenieMessageRef) -> str:
        return (
            f"spaces/{quote(ref['space_id'], safe='')}/conversations/"
            f"{quote(ref['conversation_id'], safe='')}/messages/{quote(ref['message_id'], safe='')}"
        )

    def _get_message(self, ref: GenieMessageRef, deadline: float | None = None) -> dict[str, Any]:
        return self.hook._request("GET", self._message_path(ref), retry_read=True, deadline=deadline)

    def get_job_status(self, external_id: Any, context: Context) -> str:
        if not isinstance(external_id, dict) or external_id.get("fingerprint") != self._fingerprint():
            self.log.warning("Stored Genie message was for a different question; asking again")
            return _STALE
        try:
            message = self._get_message(external_id)
        except DatabricksGenieError as err:
            if err.status_code == 404:
                return _NOT_FOUND
            raise
        return str(message.get("status"))

    def is_job_active(self, status: str) -> bool:
        return status in _PENDING_STATUSES

    def is_job_succeeded(self, status: str) -> bool:
        return status == "COMPLETED"

    def _check_message(self, message: dict[str, Any], ref: GenieMessageRef) -> bool:
        """Return whether the message is completed, raise if it ended without an answer."""
        status = message.get("status")
        if status == "COMPLETED":
            return True
        if status in _PENDING_STATUSES:
            return False
        if status == "FAILED":
            error = message.get("error") or {}
            detail = error.get("error") if isinstance(error, dict) else str(error)
            raise DatabricksGenieError(
                f"Genie could not answer message {ref['message_id']}: {detail or 'unknown error'}"
            )
        if status in {"CANCELLED", "QUERY_RESULT_EXPIRED"}:
            raise DatabricksGenieError(f"Genie message {ref['message_id']} ended with status {status}.")
        raise DatabricksGenieError(f"Genie returned an unknown message status: {status!r}.")

    def poll_until_complete(self, external_id: Any, context: Context) -> Any:
        """
        Wait for the message and return its bounded result.

        ``execute_resumable`` returns this value directly when it reconnects to an
        active message, so polling returns the result instead of ``None``.
        """
        ref: GenieMessageRef = external_id
        deadline = time.monotonic() + self.timeout
        message = self._get_message(ref, deadline)
        if not self._check_message(message, ref) and self.deferrable:
            self.log.info("Deferring until Genie message %s completes", ref["message_id"])
            self.defer(
                trigger=DatabricksGenieMessageTrigger(
                    space_id=ref["space_id"],
                    conversation_id=ref["conversation_id"],
                    message_id=ref["message_id"],
                    databricks_conn_id=self.databricks_conn_id,
                    polling_period_seconds=self.polling_period_seconds,
                    retry_limit=self.databricks_retry_limit,
                    retry_delay=self.databricks_retry_delay,
                ),
                method_name="execute_complete",
                timeout=timedelta(seconds=self.timeout),
                kwargs={"ref": ref},
            )
        while not self._check_message(message, ref):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DatabricksGenieError(
                    f"Timed out after {self.timeout} seconds waiting for Genie message "
                    f"{ref['message_id']}; the next try reconnects to it."
                )
            time.sleep(min(self.polling_period_seconds, remaining))
            message = self._get_message(ref, deadline)
        return self._summarize(message, ref, deadline)

    def _summarize(self, message: dict[str, Any], ref: GenieMessageRef, deadline: float) -> dict[str, Any]:
        result = self.hook._summarize_result(
            ref["space_id"],
            quote(ref["space_id"], safe=""),
            ref["conversation_id"],
            ref["message_id"],
            message,
            deadline,
        )
        self._result = _truncate_result(result)
        return self._result

    def get_job_result(self, external_id: Any, context: Context) -> dict[str, Any]:
        ref: GenieMessageRef = external_id
        if self._result is not None and self._result.get("message_id") == ref["message_id"]:
            return self._result
        deadline = time.monotonic() + self.timeout
        message = self._get_message(ref, deadline)
        if not self._check_message(message, ref):
            raise DatabricksGenieError(f"Genie message {ref['message_id']} has not completed yet.")
        return self._summarize(message, ref, deadline)

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None, *, ref: GenieMessageRef
    ) -> dict[str, Any]:
        if (
            not event
            or event.get("conversation_id") != ref["conversation_id"]
            or event.get("message_id") != ref["message_id"]
            or event.get("status") not in ("success", "error")
        ):
            raise DatabricksGenieError(f"Genie message {ref['message_id']} trigger returned an invalid event")
        if event["status"] == "error":
            status_code = event.get("status_code")
            raise DatabricksGenieError(
                f"Polling Genie message {ref['message_id']} failed ({event.get('error_type')}); "
                "inspect the triggerer logs. The next try reconnects to the message.",
                status_code=status_code if isinstance(status_code, int) else None,
            )
        return self.get_job_result(ref, context)
