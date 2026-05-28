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
from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
from contextlib import contextmanager, suppress
from functools import wraps
from typing import TYPE_CHECKING, Any

from asgiref.sync import sync_to_async

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.common.compat.connection import get_async_connection
from airflow.providers.common.compat.sdk import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Connection


# Guarded, module-level import for the optional heavy `ibmmq` C extension.
# Providers should avoid importing optional dependencies at module import time
# to keep Airflow lightweight for users who don't need the provider. Importing
# inline in every function is repetitive; perform a single guarded import and
# raise a clear error on first use.
try:
    import ibmmq  # type: ignore
except (ImportError, ModuleNotFoundError):  # missing optional dependency
    ibmmq = None  # type: ignore


# Backoff parameters for transient consume failures
_BACKOFF_BASE: float = 1.0
_BACKOFF_MAX: float = 60.0
_BACKOFF_FACTOR: float = 2.0
_TRANSIENT_REASON_NAMES = frozenset(
    {
        "MQRC_CONNECTION_BROKEN",
        "MQRC_Q_MGR_QUIESCING",
        "MQRC_Q_MGR_NOT_AVAILABLE",
        "MQRC_HOST_NOT_AVAILABLE",
        "MQRC_CONNECTION_QUIESCING",
    }
)
# Sentinel values used when the wrapped exception is not an MQMIError and
# therefore doesn't expose MQ completion/reason codes. Using -1 (instead of 0)
# avoids colliding with a legitimate MQ reason/completion code of 0 and
# allows downstream consumers (logs, Sentry tags) to distinguish non-MQ errors.
_NON_MQ_SENTINEL: int = -1


def requires_ibmmq(func):
    """
    Ensure the optional ``ibmmq`` module is available.

    Use this decorator on functions or methods that call into the native ``ibmmq`` extension so callers
    receive a clear :class:`AirflowOptionalProviderFeatureException` when the dependency is not installed.
    """

    @wraps(func)
    def _wrapped(*args, **kwargs):
        if ibmmq is None:
            raise AirflowOptionalProviderFeatureException(
                "The 'ibmmq' package is required to use the IBM MQ provider. "
                "Install the provider extra (e.g. 'apache-airflow-providers-ibm[ibm-mq]') "
                "or add the 'ibmmq' dependency to your environment."
            )
        return func(*args, **kwargs)

    return _wrapped


class IBMMQError(Exception):
    """
    Lightweight wrapper for IBM MQ errors raised by the consumer thread.

    This allows the async event-loop code in :meth:`IBMMQHook.aconsume` to
    handle MQ errors **without importing the heavy ibmmq C extension** on the
    event-loop thread.

    :param reason: The integer MQ reason code (e.g. ``MQRC_CONNECTION_BROKEN``).
    :param comp: The integer MQ completion code.
    :note: When ``reason``/``comp`` equal ``_NON_MQ_SENTINEL`` (``-1``), the
        error was not produced by an ``ibmmq.MQMIError`` and no MQ codes are
        available (for example ``ibmmq.PYIFError`` or a wrapped
        ``ConnectionError``).
    :param transient: Whether this error is considered transient (eligible for retry).
    :param message: Human-readable description of the error.
    """

    def __init__(self, reason: int, comp: int, transient: bool, message: str):
        super().__init__(message)
        self.reason = reason
        self.comp = comp
        self.transient = transient


class IBMMQConsumer(threading.Thread, LoggingMixin):
    """
    Thread worker that consumes one message from an IBM MQ queue.

    The consumer is used by :meth:`IBMMQHook.aconsume` to execute blocking MQ
    calls in a dedicated thread because the IBM MQ C client requires handles to
    be used from the thread that created them. The result (or exception) is
    forwarded to the asyncio event loop through ``future``.

    :param hook: Hook instance that provides connection and queue helpers.
    :param connection: Airflow connection object resolved from ``hook.conn_id``.
    :param queue_name: Queue to consume from.
    :param poll_interval: Maximum wait time (seconds) for each ``q.get`` call.
    :param loop: Event loop that owns ``future``.
    :param future: Future completed with the decoded message or an exception.
    :param stop_event: Signal used to stop polling after cancellation.
    """

    def __init__(
        self,
        hook: IBMMQHook,
        connection: Connection,
        queue_name: str,
        poll_interval: float,
        loop: asyncio.AbstractEventLoop,
        future: asyncio.Future,
        stop_event: threading.Event,
    ):
        super().__init__(daemon=True)
        self.hook = hook
        self.connection = connection
        self.queue_name = queue_name
        self.poll_interval = poll_interval
        self.loop = loop
        self.future = future
        self.stop_event = stop_event

    @requires_ibmmq
    def _process_message(self, message: bytes) -> str:
        """
        Process a raw MQ message.

        If the message contains an RFH2 header, the header is unpacked and the
        payload following the header is returned. If unpacking fails, the raw
        message is returned decoded as UTF-8.

        Because the message is consumed with ``MQGMO_NO_SYNCPOINT``, it has already
        been removed from the queue. Any exception raised here would lose the message,
        so we catch all errors and fall back to returning the raw message.

        :param message: Raw message received from IBM MQ.
        :return: Decoded message payload.
        """
        try:
            rfh2 = ibmmq.RFH2()
            rfh2.unpack(message)

            payload_offset = rfh2.get_length()
            # Defensive guard: if offset is out of bounds, fall back to raw message
            if payload_offset >= len(message):
                self.log.warning(
                    "RFH2 offset %d exceeds message length %d; returning raw message",
                    payload_offset,
                    len(message),
                )
                return message.decode("utf-8", errors="ignore")

            payload = message[payload_offset:]
            decoded = payload.decode("utf-8", errors="ignore")
            if self.log.isEnabledFor(logging.DEBUG):
                truncated_decoded = decoded[:200] + ("..." if len(decoded) > 200 else "")
                self.log.debug("Message received from MQ (RFH2 decoded): %s", truncated_decoded)
            return decoded
        except Exception as error:
            # Catch all exceptions (PYIFError, struct errors, etc.) to avoid losing messages.
            # Since message is already removed from queue (MQGMO_NO_SYNCPOINT), log and return raw.
            self.log.warning(
                "Failed to process RFH2 header (%s: %s) for message (size: %d bytes); returning raw message",
                type(error).__name__,
                error,
                len(message),
            )
            if self.log.isEnabledFor(logging.DEBUG):
                truncated_message = message.decode("utf-8", errors="ignore")[:200]
                truncated_message_display = truncated_message + ("..." if len(message) > 200 else "")
                self.log.debug("Raw message payload (truncated): %s", truncated_message_display)
            return message.decode("utf-8", errors="ignore")

    @requires_ibmmq
    def consume(
        self,
        queue_name: str,
        poll_interval: float,
        stop_event: threading.Event,
    ) -> str | None:
        """
        Blocking implementation that consumes a single message from the given IBM MQ queue.

        All IBM MQ handles (queue manager connection, queue) are created **and used** within
        this method, satisfying the thread-affinity requirement of the IBM MQ C client library.
        The 'stop_event' is checked between 'q.get' calls so the thread terminates promptly
        after the coroutine side is canceled.  Reads are performed with
        ``MQGMO_NO_SYNCPOINT``, so this method provides at-most-once delivery:
        once ``q.get`` returns successfully, IBM MQ has already committed the
        message removal from the queue.

        MQ-specific exceptions are caught and re-raised as :class:`IBMMQError` so that
        the async caller never needs to import the heavy ``ibmmq`` C extension.

        For an asynchronous interface see :meth:`IBMMQHook.aconsume`.
        """
        transient_reasons = frozenset(
            getattr(ibmmq.CMQC, name) for name in _TRANSIENT_REASON_NAMES if hasattr(ibmmq.CMQC, name)
        )

        od = ibmmq.OD()
        od.ObjectName = queue_name

        md = ibmmq.MD()
        md.Format = ibmmq.CMQC.MQFMT_STRING
        md.CodedCharSetId = 1208
        md.Encoding = ibmmq.CMQC.MQENC_NATIVE

        gmo = ibmmq.GMO()
        gmo.Options = ibmmq.CMQC.MQGMO_WAIT | ibmmq.CMQC.MQGMO_NO_SYNCPOINT | ibmmq.CMQC.MQGMO_CONVERT
        gmo.WaitInterval = int(poll_interval * 1000)

        try:
            with self.hook.get_conn(connection=self.connection) as conn:
                q = ibmmq.Queue(conn, od, self.hook.get_open_options(queue_name=queue_name))
                try:
                    # WaitInterval already blocks for poll_interval seconds when no message is
                    # available, so no additional sleep is needed between iterations.
                    while not stop_event.is_set():
                        try:
                            message = q.get(None, md, gmo)
                            if message:
                                return self._process_message(message)
                        except ibmmq.MQMIError as e:
                            if e.reason == ibmmq.CMQC.MQRC_NO_MSG_AVAILABLE:
                                self.log.info(
                                    "No message available on queue '%s' (reason=%s)",
                                    queue_name,
                                    e.reason,
                                )
                                continue
                            self.log.error(
                                "IBM MQ error on queue '%s': completion_code=%s reason_code=%s (%s)",
                                queue_name,
                                e.comp,
                                e.reason,
                                e,
                            )
                            raise
                finally:
                    with suppress(Exception):
                        q.close()
        except ibmmq.MQMIError as e:
            raise IBMMQError(
                reason=e.reason,
                comp=e.comp,
                transient=e.reason in transient_reasons,
                message=str(e),
            ) from e
        except ibmmq.PYIFError as e:
            raise IBMMQError(
                reason=_NON_MQ_SENTINEL,
                comp=_NON_MQ_SENTINEL,
                transient=False,
                message=str(e),
            ) from e
        except ConnectionError as e:
            # _connect() wraps ibmmq.MQMIError as ConnectionError; treat as transient
            # so aconsume retries with backoff instead of killing the trigger.
            raise IBMMQError(
                reason=_NON_MQ_SENTINEL,
                comp=_NON_MQ_SENTINEL,
                transient=True,
                message=str(e),
            ) from e
        return None

    def run(self):
        try:
            result = self.consume(
                queue_name=self.queue_name,
                poll_interval=self.poll_interval,
                stop_event=self.stop_event,
            )

            if not self.future.cancelled():
                self.loop.call_soon_threadsafe(self.future.set_result, result)
        except Exception as e:
            if not self.future.cancelled():
                self.loop.call_soon_threadsafe(self.future.set_exception, e)


class IBMMQHook(BaseHook):
    """
    Interact with IBM MQ queue managers to consume and produce messages.

    This hook wraps the ``ibmmq`` C client and manages connection
    lifecycle, queue open/close, and message serialization.  Both synchronous
    (context-manager) and asynchronous (``consume`` / ``produce``) interfaces
    are provided.

    The asynchronous consume path intentionally uses ``MQGMO_NO_SYNCPOINT``.
    That keeps reads non-transactional and therefore provides **at-most-once**
    delivery semantics for trigger-based consumption: once ``q.get`` returns,
    IBM MQ has already removed the message from the queue.  If the coroutine is
    canceled after that point but before Airflow yields a ``TriggerEvent``, the
    message can be lost.  The stop-event machinery only prevents additional
    ``q.get`` calls after cancellation; it cannot make the non-transactional
    get atomic with TriggerEvent emission.

    Connection parameters (host, port, login, password) are read from the
    Airflow connection identified by *conn_id*.  ``queue_manager``, ``channel``,
    and ``open_options`` can be supplied either as constructor arguments or via
    the connection's *extra* JSON — constructor arguments take precedence.

    :param conn_id: Airflow connection ID for the IBM MQ instance.
        Defaults to ``"mq_default"``.
    :param queue_manager: Name of the IBM MQ queue manager to connect to.
        If not provided, the value is read from the ``queue_manager`` key in
        the connection's *extra* JSON.
    :param channel: MQ channel name used for the connection.
        If not provided, the value is read from the ``channel`` key in the
        connection's *extra* JSON.
    :param open_options: Integer bitmask of ``MQOO_*`` open options passed
        when opening a queue (e.g.,
        ``ibmmq.CMQC.MQOO_INPUT_SHARED | ibmmq.CMQC.MQOO_FAIL_IF_QUIESCING``).
        If not provided, the value is resolved from the ``open_options`` key
        in the connection's *extra* JSON, falling back to
        ``MQOO_INPUT_SHARED``.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "mq_default"
    conn_type = "ibmmq"
    hook_name = "IBM MQ"
    default_open_options = "MQOO_INPUT_SHARED"

    def __init__(
        self,
        conn_id: str = default_conn_name,
        queue_manager: str | None = None,
        channel: str | None = None,
        open_options: int | None = None,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.queue_manager = queue_manager
        self.channel = channel
        self.open_options = open_options

    @classmethod
    @requires_ibmmq
    def parse_open_options(cls, value) -> int:
        """
        Parse MQ open-options from allowed formats into an integer bitmask.

        Accepts:
        - int (returned as-is)
        - numeric string (decimal or hex, e.g., "2" or "0x10")
        - single symbol name from ``ibmmq.CMQC`` (e.g., "MQOO_INPUT_SHARED")
        - pipe- or comma-separated symbols (e.g. "MQOO_INPUT_SHARED | MQOO_FAIL_IF_QUIESCING")

        Raises ValueError on unknown symbol tokens and TypeError for unsupported types.
        """
        if value is None:
            return ibmmq.CMQC.MQOO_INPUT_SHARED

        if isinstance(value, int):
            return value

        if isinstance(value, str):
            s = value.strip()
            # Try numeric literal first (decimal or hex)
            with suppress(ValueError):
                if s.startswith(("0x", "0X")):
                    return int(s, 16)
                return int(s)

            tokens = [t.strip() for t in re.split(r"\s*(?:\||,)\s*", s) if t.strip()]
            if not tokens:
                raise ValueError("Empty open_options string")

            result = 0
            for token in tokens:
                if hasattr(ibmmq.CMQC, token):
                    result |= getattr(ibmmq.CMQC, token)
                else:
                    raise ValueError(f"Unknown MQ open option token: {token}")
            return result

        raise TypeError("open_options must be an int or string of MQOO_* tokens")

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for IBM MQ Connection."""
        return {
            "hidden_fields": ["schema"],
            "placeholders": {
                "host": "mq.example.com",
                "port": "1414",
                "login": "app_user",
                "extra": json.dumps(
                    {
                        "queue_manager": "QM1",
                        "channel": "DEV.APP.SVRCONN",
                        "open_options": cls.default_open_options,
                    },
                    indent=2,
                ),
            },
        }

    @classmethod
    @requires_ibmmq
    def get_open_options_flags(cls, open_options: int) -> list[str]:
        """
        Return the symbolic MQ open option flags set in a given bitmask.

        Each flag corresponds to a constant in ``ibmmq.CMQC`` that starts with ``MQOO_``.

        :param open_options: The integer bitmask used when opening an MQ queue
                             (e.g., ``MQOO_INPUT_EXCLUSIVE | MQOO_FAIL_IF_QUIESCING``).

        :return: A list of the names of the MQ open flags that are set in the bitmask.
                 For example, ``['MQOO_INPUT_EXCLUSIVE', 'MQOO_FAIL_IF_QUIESCING']``.

        Example:
            >>> open_options = ibmmq.CMQC.MQOO_INPUT_SHARED | ibmmq.CMQC.MQOO_FAIL_IF_QUIESCING
            >>> cls.get_open_options_flags(open_options)
            ['MQOO_INPUT_SHARED', 'MQOO_FAIL_IF_QUIESCING']
        """
        return [
            name
            for name, value in vars(ibmmq.CMQC).items()
            if name.startswith("MQOO_") and (open_options & value)
        ]

    def get_open_options(self, queue_name: str) -> int | None:
        # Prefer a resolved value set by ``get_conn`` during the connection
        # context; fall back to the instance attribute if present.
        open_options_val = getattr(self, "_resolved_open_options", None)
        if open_options_val is None:
            open_options_val = self.open_options

        if open_options_val is not None:
            flag_names = self.get_open_options_flags(open_options_val)
            self.log.info(
                "Opening MQ queue '%s' with open_options=%s (%s)",
                queue_name,
                open_options_val,
                ", ".join(flag_names),
            )
        return open_options_val

    @staticmethod
    @requires_ibmmq
    def _connect(queue_manager: str, channel: str, conn_info: str, csp):
        """
        Connect to the IBM MQ queue manager.

        Connection errors from the C client are caught and re-raised as a
        :class:`ConnectionError` with a human-readable message.

        :return: IBM MQ connection object
        """
        try:
            return ibmmq.connect(queue_manager, channel, conn_info, csp=csp)
        except (ibmmq.MQMIError, ibmmq.PYIFError) as e:
            raise ConnectionError(
                f"Failed to connect to IBM MQ queue manager '{queue_manager}' "
                f"at {conn_info} on channel '{channel}': {e}"
            ) from e

    @contextmanager
    @requires_ibmmq
    def get_conn(self, connection: Connection | None = None):
        """
        Sync context manager for IBM MQ connection lifecycle.

        Must be called from the executor thread (not the event loop thread).
        Retrieves the Airflow connection (or uses the explicitly supplied one),
        extracts MQ parameters, and manages the IBM MQ connection lifecycle.

        :param connection: Optional Airflow connection object. When omitted,
            the connection is resolved from ``self.conn_id``.
        :yield: IBM MQ connection object
        """
        connection = connection or BaseHook.get_connection(self.conn_id)
        config = connection.extra_dejson
        queue_manager = self.queue_manager or config.get("queue_manager")
        channel = self.channel or config.get("channel")

        if not queue_manager:
            raise ValueError("queue_manager must be set in Connection extra config or hook init")
        if not channel:
            raise ValueError("channel must be set in Connection extra config or hook init")

        # Resolve open_options without mutating the hook instance so the
        # connection remains idempotent across calls. The temporary resolved
        # value is stored on the instance for the duration of the context
        # manager and removed afterward.
        if self.open_options is None:
            config_value = config.get("open_options", self.default_open_options)
            # Use the class-level parser so callers (and tests) can reuse the
            # logic without importing a module-private helper.
            resolved_open_options = self.parse_open_options(config_value)
        else:
            resolved_open_options = self.open_options

        # Store the resolved value temporarily for get_open_options to pick up
        # while inside the connection context.
        self._resolved_open_options = resolved_open_options

        csp = ibmmq.CSP()
        csp.CSPUserId = connection.login
        csp.CSPPassword = connection.password

        conn_info = f"{connection.host}({connection.port})"
        conn = self._connect(queue_manager, channel, conn_info, csp)
        try:
            yield conn
        finally:
            # Remove the temporary resolved value so the hook instance is
            # unchanged after the context exits.
            with suppress(Exception):
                delattr(self, "_resolved_open_options")
            with suppress(Exception):
                conn.disconnect()

    async def aconsume(self, queue_name: str, poll_interval: float = 5) -> str | None:
        """
        Asynchronous version of :meth:`consume`.

        Wait for a single message from the specified IBM MQ queue and return its decoded payload.

        The method retries with exponential back-off whenever the underlying
        :meth:`consume` returns ``None`` (connection broken, timeout) or raises
        an unexpected exception, so that an AssetWatcher is never silently killed
        by a transient failure.

        All blocking IBM MQ operations ('connect', 'open', 'get', 'close', 'disconnect') run in a
        separate thread via 'sync_to_async' to satisfy the IBM MQ C client's thread-affinity
        requirement — every operation on a connection must happen from the thread that created it.

        A :class:`threading.Event` stop signal is passed to the worker so that, when this
        coroutine is canceled (e.g. because the Airflow triggerer reassigns the watcher to
        another pod), the background thread exits cleanly after the current 'q.get' call
        times out (at most 'poll_interval' seconds).  This prevents orphaned
        threads from continuing to poll after cancellation, but it does **not**
        change the delivery guarantee: :meth:`consume` uses
        ``MQGMO_NO_SYNCPOINT``, so cancellation after ``q.get`` returns but
        before the trigger yields an event can still lose that message.

        :param queue_name: Name of the IBM MQ queue to consume messages from.
        :param poll_interval: Interval in seconds used to wait for messages and to control
            how long the underlying MQ 'get' operation blocks before checking again.
        :return: The decoded message payload.
        """
        connection = await get_async_connection(self.conn_id)
        backoff = _BACKOFF_BASE
        while True:
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            stop_event = threading.Event()
            thread = IBMMQConsumer(
                hook=self,
                connection=connection,
                queue_name=queue_name,
                poll_interval=poll_interval,
                loop=loop,
                future=future,
                stop_event=stop_event,
            )
            thread.start()

            try:
                result = await future

                if result is not None:
                    return result
            except asyncio.CancelledError:
                stop_event.set()
                raise
            except IBMMQError as e:
                if e.transient:
                    self.log.warning(
                        "Transient MQ error on queue '%s': completion_code=%s reason_code=%s (%s); retrying in %.1fs",
                        queue_name,
                        e.comp,
                        e.reason,
                        e,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * _BACKOFF_FACTOR, _BACKOFF_MAX)
                    continue
                self.log.error(
                    "Permanent MQ error on queue '%s': completion_code=%s reason_code=%s (%s) -- not retrying",
                    queue_name,
                    e.comp,
                    e.reason,
                    e,
                )
                raise
            except Exception:
                # Programming errors should not be retried
                self.log.exception(
                    "Unexpected error in IBM MQ consume for queue '%s' -- not retrying",
                    queue_name,
                )
                raise
            finally:
                stop_event.set()
                thread.join(timeout=poll_interval + 1)

            self.log.info(
                "IBM MQ consume returned no event for queue '%s'; queue may be quiet. Retrying in %.1fs",
                queue_name,
                backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * _BACKOFF_FACTOR, _BACKOFF_MAX)

    async def aproduce(self, queue_name: str, payload: str, open_options: int | None = None) -> None:
        """
        Asynchronous version of :meth:`produce`.

        Put a message onto the specified IBM MQ queue.

        All blocking IBM MQ operations run in a separate thread via 'sync_to_async' for the same
        thread-safety reasons as :meth:`aconsume`.

        :param queue_name: Name of the IBM MQ queue to which the message should be sent.
        :param payload: Message payload to send. The payload will be encoded as UTF-8
            before being placed on the queue.
        :param open_options: Integer bitmask of ``MQOO_*`` open options for the queue.
            If not provided, defaults to ``MQOO_OUTPUT``.
        :return: None
        """
        connection = await get_async_connection(self.conn_id)
        await sync_to_async(self.produce, thread_sensitive=False)(
            connection, queue_name, payload, open_options
        )

    @requires_ibmmq
    def produce(
        self,
        connection: Connection,
        queue_name: str,
        payload: str,
        open_options: int | None = None,
    ) -> None:
        """
        Blocking implementation of :meth:`aproduce`.

        :param connection: Airflow connection object.
        :param queue_name: Name of the IBM MQ queue to which the message should be sent.
        :param payload: Message payload to send. The payload will be encoded as UTF-8
            before being placed on the queue.
        :param open_options: Integer bitmask of ``MQOO_*`` open options for the queue.
            If not provided, defaults to ``MQOO_OUTPUT``.
        """
        od = ibmmq.OD()
        od.ObjectName = queue_name

        md = ibmmq.MD()
        md.Format = ibmmq.CMQC.MQFMT_STRING
        md.CodedCharSetId = 1208
        md.Encoding = ibmmq.CMQC.MQENC_NATIVE

        if open_options is None:
            open_options = ibmmq.CMQC.MQOO_OUTPUT

        try:
            with self.get_conn(connection=connection) as conn:
                q = ibmmq.Queue(conn, od, open_options)
                try:
                    q.put(payload.encode("utf-8"), md)
                finally:
                    with suppress(Exception):
                        q.close()
        except Exception:
            self.log.exception(
                "MQ produce failed on queue '%s'",
                queue_name,
            )
            raise
