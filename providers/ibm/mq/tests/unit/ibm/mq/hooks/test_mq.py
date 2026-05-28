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
import logging
import operator
import struct
import threading
from functools import reduce
from itertools import count
from typing import Any
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.ibm.mq.hooks.mq import (
    _BACKOFF_BASE,
    _BACKOFF_FACTOR,
    _BACKOFF_MAX,
    _NON_MQ_SENTINEL,
    IBMMQConsumer,
    IBMMQError,
    IBMMQHook,
)

MQ_PAYLOAD = """RFH x"MQSTR    <mcd><Msd>jms_map</Msd></mcd>   <jms><Dst>topic://localhost/topic</Dst><Tms>1772121947476</Tms><Dlv>2</Dlv><Uci dt='bin.hex'>414D5143514D49413030542020202069774D7092F81057</Uci></jms>L<usr><XMSC_CLIENT_ID>local</XMSC_CLIENT_ID><release>26.01.00</release></usr> 4<mqps><Top>topic</Top></mqps>  {}"""


def mq_connection(open_options: Any = None):
    """Create a test MQ connection object."""
    import json

    extra: dict[str, Any] = {"queue_manager": "QM1", "channel": "DEV.APP.SVRCONN"}

    if open_options is not None:
        extra["open_options"] = open_options

    return Connection(
        conn_id="mq_conn",
        conn_type="ibmmq",
        host="mq.example.com",
        login="user",
        password="pass",
        port=1414,
        extra=json.dumps(extra),
    )


@pytest.fixture
def mock_get_connection():
    """Fixture that mocks get_async_connection to return a test connection."""
    with patch(
        "airflow.providers.ibm.mq.hooks.mq.get_async_connection",
        new_callable=AsyncMock,
    ) as mock_conn:
        mock_conn.return_value = mq_connection()
        yield mock_conn


@pytest.fixture
def mock_base_get_connection():
    """Fixture that mocks BaseHook.get_connection to return a test connection."""
    with patch("airflow.providers.ibm.mq.hooks.mq.BaseHook.get_connection") as mock_conn:
        mock_conn.return_value = mq_connection()
        yield mock_conn


@pytest.fixture
def patch_sync_to_async():
    """Patch sync_to_async to call the wrapped function directly for testing."""

    def sync_to_async(func, **kwargs):
        """Wrap a sync function so it can be awaited directly."""

        async def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    with patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async", side_effect=sync_to_async):
        yield


def fake_get(*args, **kwargs):
    import ibmmq

    raise ibmmq.MQMIError(comp=ibmmq.CMQC.MQCC_FAILED, reason=ibmmq.CMQC.MQRC_CONNECTION_BROKEN)


@pytest.mark.asyncio
class TestIBMMQHook:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="mq_conn",
                conn_type="mq",
                host="mq.example.com",
                login="user",
                password="pass",
                port=1414,
                extra='{"queue_manager": "QM1", "channel": "DEV.APP.SVRCONN"}',
            )
        )
        self.hook = IBMMQHook("mq_conn")

    @pytest.mark.parametrize(
        ("open_options_attr", "use_explicit_connection"),
        [
            pytest.param("MQGMO_NO_WAIT", True, id="explicit_connection_no_wait"),
            pytest.param("MQGMO_NO_WAIT", False, id="hook_connection_no_wait"),
            pytest.param("MQOO_INPUT_EXCLUSIVE", True, id="explicit_connection_input_exclusive"),
            pytest.param("MQOO_INPUT_EXCLUSIVE", False, id="hook_connection_input_exclusive"),
            pytest.param("MQOO_INPUT_SHARED", True, id="explicit_connection_input_shared"),
            pytest.param("MQOO_INPUT_SHARED", False, id="hook_connection_input_shared"),
        ],
    )
    async def test_get_conn_with_open_options(
        self,
        open_options_attr,
        use_explicit_connection,
        mock_base_get_connection,
    ):
        import ibmmq

        open_options = getattr(ibmmq.CMQC, open_options_attr)
        hook = IBMMQHook("mq_conn")
        mock_conn = MagicMock()

        assert not hook.open_options

        hook.open_options = open_options
        with patch.object(hook, "_connect", return_value=mock_conn) as mock_connect:
            if use_explicit_connection:
                with hook.get_conn(connection=mq_connection()):
                    pass
            else:
                with hook.get_conn():
                    pass

            assert hook.open_options == open_options

        mock_connect.assert_called_once()
        mock_conn.disconnect.assert_called_once()
        if use_explicit_connection:
            mock_base_get_connection.assert_not_called()
        else:
            mock_base_get_connection.assert_called_once_with("mq_conn")

    @pytest.mark.parametrize(
        ("open_options", "expect_exception", "expected_resolved"),
        [
            (3, False, 3),
            ("MQOO_INPUT_EXCLSUVE", True, None),
        ],
    )
    def test_get_conn_resolves_or_errors_based_on_connection_extra(
        self, open_options, expect_exception, expected_resolved
    ):
        hook = IBMMQHook(open_options=None)
        mock_conn = MagicMock()

        with patch.object(hook, "_connect", return_value=mock_conn) as mock_connect:
            if expect_exception:
                with pytest.raises(ValueError, match="Unknown MQ open option token"):
                    with hook.get_conn(connection=mq_connection(open_options=open_options)):
                        pass
                assert hook.open_options is None

                mock_connect.assert_not_called()
                mock_conn.disconnect.assert_not_called()
            else:
                with hook.get_conn(connection=mq_connection(open_options=open_options)):
                    assert hasattr(hook, "_resolved_open_options")
                    assert getattr(hook, "_resolved_open_options") == expected_resolved

                assert not hasattr(hook, "_resolved_open_options")
                assert hook.open_options is None

                mock_connect.assert_called_once()
                mock_conn.disconnect.assert_called_once()

    @pytest.mark.parametrize(
        ("open_options_attr", "expected_flags"),
        [
            ("MQOO_INPUT_EXCLUSIVE", ["MQOO_INPUT_EXCLUSIVE"]),
            ("MQOO_INPUT_SHARED", ["MQOO_INPUT_SHARED"]),
            (
                "MQOO_INPUT_SHARED | MQOO_FAIL_IF_QUIESCING",
                ["MQOO_INPUT_SHARED", "MQOO_FAIL_IF_QUIESCING"],
            ),
        ],
    )
    async def test_get_open_options_flags(self, open_options_attr, expected_flags):
        import ibmmq

        open_options = [
            getattr(ibmmq.CMQC, open_option.strip()) for open_option in open_options_attr.split("|")
        ]
        combined_options = reduce(operator.or_, open_options)
        flags = IBMMQHook.get_open_options_flags(combined_options)

        assert flags == expected_flags

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, 2),
            (3, 3),
            ("0x10", 0x10),
            ("42", 42),
            ("MQOO_INPUT_SHARED", 2),
            ("MQOO_INPUT_SHARED | MQOO_FAIL_IF_QUIESCING", 2 | 8192),
            ("MQOO_INPUT_SHARED, MQOO_FAIL_IF_QUIESCING", 2 | 8192),
            ("   MQOO_INPUT_SHARED   |   MQOO_FAIL_IF_QUIESCING  ", 2 | 8192),
        ],
    )
    def test_parse_open_options_accepts_expected_formats(self, value, expected):
        result = IBMMQHook.parse_open_options(value)
        assert result == expected

    @pytest.mark.parametrize("bad_value", ["", "UNKNOWN_FLAG", [1, 2]])
    def test_parse_open_options_rejects_invalid_inputs(self, bad_value):
        if isinstance(bad_value, list):
            with pytest.raises(TypeError):
                IBMMQHook.parse_open_options(bad_value)
        else:
            with pytest.raises(ValueError, match=r"Unknown MQ open option token|Empty open_options string"):
                IBMMQHook.parse_open_options(bad_value)

    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    async def test_aconsume_message(
        self, mock_queue_class, mock_connect, mock_get_connection, patch_sync_to_async
    ):
        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue
        mock_queue.get.return_value = MQ_PAYLOAD.format("test message").encode()

        result = await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)
        assert isinstance(result, str)
        assert "test message" in result

        mock_connect.assert_called_once()
        mock_queue_class.assert_called_once_with(
            mock_qmgr,
            mock.ANY,
            mock.ANY,
        )

    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    async def test_aproduce_message(
        self, mock_queue_class, mock_connect, mock_get_connection, patch_sync_to_async
    ):
        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        await self.hook.aproduce(queue_name="QUEUE1", payload="payload")

        mock_connect.assert_called_once()
        mock_queue_class.assert_called_once_with(
            mock_qmgr,
            mock.ANY,
            mock.ANY,
        )
        mock_queue.put.assert_called_once()

    @pytest.mark.parametrize(
        ("open_options_attr", "use_explicit_open_options"),
        [
            pytest.param("MQOO_OUTPUT", False, id="default_output"),
            pytest.param(
                "MQOO_OUTPUT | MQOO_FAIL_IF_QUIESCING", True, id="custom_output_with_fail_if_quiescing"
            ),
        ],
    )
    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    async def test_aproduce_with_custom_open_options(
        self,
        mock_queue_class,
        mock_connect,
        mock_get_connection,
        patch_sync_to_async,
        open_options_attr,
        use_explicit_open_options,
    ):
        import ibmmq

        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        # Calculate the expected open_options value
        open_options_values = [getattr(ibmmq.CMQC, opt.strip()) for opt in open_options_attr.split("|")]
        expected_open_options = reduce(operator.or_, open_options_values)

        if use_explicit_open_options:
            await self.hook.aproduce(
                queue_name="QUEUE1", payload="payload", open_options=expected_open_options
            )
        else:
            await self.hook.aproduce(queue_name="QUEUE1", payload="payload")
            # When not specified, should default to MQOO_OUTPUT
            expected_open_options = ibmmq.CMQC.MQOO_OUTPUT

        mock_connect.assert_called_once()
        # Verify Queue was called with the expected open_options
        call_args = mock_queue_class.call_args
        actual_open_options = call_args[0][2]  # Third positional argument is open_options
        assert actual_open_options == expected_open_options
        mock_queue.put.assert_called_once()

    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    async def test_aconsume_connection_broken(
        self, mock_queue_class, mock_connect, mock_get_connection, patch_sync_to_async, caplog
    ):
        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr
        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue
        mock_queue.get.side_effect = fake_get

        # aconsume() retries on None, so we need to cancel after the first attempt
        with patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = asyncio.CancelledError
            with pytest.raises(asyncio.CancelledError):
                await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

        assert "Opening MQ queue 'QUEUE1' with open_options=2 (MQOO_INPUT_SHARED)" in caplog.text

    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    def test_consume_uses_no_syncpoint(self, mock_queue_class, mock_connect, mock_get_connection):
        import ibmmq

        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr
        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        stop_event = threading.Event()
        captured_options: dict[str, int] = {}

        def get_message(_buffer, _md, gmo):
            captured_options["value"] = gmo.Options
            stop_event.set()
            raise ibmmq.MQMIError(comp=ibmmq.CMQC.MQCC_WARNING, reason=ibmmq.CMQC.MQRC_NO_MSG_AVAILABLE)

        mock_queue.get.side_effect = get_message

        consumer = IBMMQConsumer(
            hook=self.hook,
            connection=mq_connection(),
            queue_name="QUEUE1",
            poll_interval=0.1,
            loop=MagicMock(),
            future=MagicMock(),
            stop_event=stop_event,
        )
        result = consumer.consume("QUEUE1", 0.1, stop_event)

        assert result is None
        assert captured_options["value"] == (
            ibmmq.CMQC.MQGMO_WAIT | ibmmq.CMQC.MQGMO_NO_SYNCPOINT | ibmmq.CMQC.MQGMO_CONVERT
        )

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_aconsume_retries_on_none_then_succeeds(self, mock_sleep, patch_sync_to_async):
        counter = count()

        def consume(
            queue_name: str,
            poll_interval: float,
            stop_event: threading.Event,
        ) -> str | None:
            assert queue_name == "QUEUE1"
            assert poll_interval == 0.1

            if stop_event.is_set():
                raise RuntimeError("Should not occur in this test!")

            if next(counter) < 2:
                return None
            return "payload after retries"

        with patch("airflow.providers.ibm.mq.hooks.mq.IBMMQConsumer.consume", side_effect=consume):
            result = await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

        assert result == "payload after retries"
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(_BACKOFF_BASE)
        mock_sleep.assert_any_call(_BACKOFF_BASE * _BACKOFF_FACTOR)

    @pytest.mark.parametrize(
        "transient_reason_name",
        [
            "MQRC_CONNECTION_BROKEN",
            "MQRC_Q_MGR_QUIESCING",
            "MQRC_Q_MGR_NOT_AVAILABLE",
            "MQRC_HOST_NOT_AVAILABLE",
            "MQRC_CONNECTION_QUIESCING",
        ],
    )
    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_aconsume_retries_on_transient_exception_then_succeeds(
        self, mock_sleep, patch_sync_to_async, transient_reason_name
    ):
        import ibmmq

        transient_reason = getattr(ibmmq.CMQC, transient_reason_name)
        transient_error = IBMMQError(
            reason=transient_reason,
            comp=ibmmq.CMQC.MQCC_FAILED,
            transient=True,
            message=f"Transient error: {transient_reason_name}",
        )
        with patch(
            "airflow.providers.ibm.mq.hooks.mq.IBMMQConsumer.consume",
            side_effect=[transient_error, "recovered"],
        ):
            result = await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

        assert result == "recovered"
        mock_sleep.assert_called_once_with(_BACKOFF_BASE)

    async def test_aconsume_does_not_retry_on_non_transient_mq_exception(self, patch_sync_to_async):
        import ibmmq

        non_transient_error = IBMMQError(
            reason=ibmmq.CMQC.MQRC_NOT_AUTHORIZED,
            comp=ibmmq.CMQC.MQCC_FAILED,
            transient=False,
            message="Not authorized",
        )
        with patch(
            "airflow.providers.ibm.mq.hooks.mq.IBMMQConsumer.consume",
            side_effect=non_transient_error,
        ):
            with pytest.raises(IBMMQError):
                await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_aconsume_backoff_caps_at_max(self, mock_sleep, patch_sync_to_async):
        failures_needed = 0
        backoff = _BACKOFF_BASE
        while backoff < _BACKOFF_MAX:
            backoff *= _BACKOFF_FACTOR
            failures_needed += 1
        failures_needed += 3

        with patch(
            "airflow.providers.ibm.mq.hooks.mq.IBMMQConsumer.consume",
            side_effect=[None] * failures_needed + ["finally"],
        ):
            result = await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

        assert result == "finally"
        capped_calls = [c for c in mock_sleep.call_args_list if c.args[0] == _BACKOFF_MAX]
        assert len(capped_calls) >= 3

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_aconsume_logs_debug_on_none(self, mock_sleep, patch_sync_to_async, caplog):
        with patch("airflow.providers.ibm.mq.hooks.mq.IBMMQConsumer.consume", side_effect=[None, "message"]):
            with caplog.at_level("DEBUG"):
                await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

        assert (
            "IBM MQ consume returned no event for queue 'QUEUE1'; queue may be quiet. Retrying in 1.0s"
            in caplog.text
        )

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_aconsume_logs_warning_on_exception(self, mock_sleep, patch_sync_to_async, caplog):
        import ibmmq

        transient_error = IBMMQError(
            reason=ibmmq.CMQC.MQRC_CONNECTION_BROKEN,
            comp=ibmmq.CMQC.MQCC_FAILED,
            transient=True,
            message="Connection broken",
        )
        with patch(
            "airflow.providers.ibm.mq.hooks.mq.IBMMQConsumer.consume",
            side_effect=[transient_error, "ok"],
        ):
            with caplog.at_level("WARNING"):
                await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

        assert "Transient MQ error on queue 'QUEUE1': completion_code=2 reason_code=2009" in caplog.text

    @patch("airflow.providers.ibm.mq.hooks.mq.asyncio.sleep", new_callable=AsyncMock)
    async def test_aconsume_cancelled_error_propagates(self, mock_sleep, patch_sync_to_async):
        mock_sleep.side_effect = asyncio.CancelledError

        with patch("airflow.providers.ibm.mq.hooks.mq.IBMMQConsumer.consume", return_value=None):
            with pytest.raises(asyncio.CancelledError):
                await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

    async def test_aconsume_propagates_non_mq_exceptions(self, mock_get_connection):
        with patch(
            "airflow.providers.ibm.mq.hooks.mq.IBMMQConsumer.consume",
            side_effect=TypeError("Unexpected programming bug"),
        ):
            with pytest.raises(TypeError, match="Unexpected programming bug"):
                await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

    @patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async")
    async def test_aconsume_does_not_call_sync_to_async(self, mock_sync_to_async, mock_get_connection):
        def fake_start(thread_self):
            thread_self.future.set_result("test message")

        with patch("airflow.providers.ibm.mq.hooks.mq.IBMMQConsumer.start", new=fake_start):
            with patch("airflow.providers.ibm.mq.hooks.mq.IBMMQConsumer.join", return_value=None):
                result = await self.hook.aconsume(queue_name="QUEUE1", poll_interval=0.1)

        assert result == "test message"
        mock_sync_to_async.assert_not_called()

    @patch("airflow.providers.ibm.mq.hooks.mq.sync_to_async")
    async def test_aproduce_calls_sync_to_async_with_thread_sensitive_false(self, mock_sync_to_async):
        mock_wrapper = AsyncMock()
        mock_sync_to_async.return_value = mock_wrapper

        await self.hook.aproduce(queue_name="QUEUE1", payload="test payload")

        mock_sync_to_async.assert_called_once()
        call_args = mock_sync_to_async.call_args
        assert call_args[1].get("thread_sensitive") is False
        assert call_args[0][0] == self.hook.produce

    @pytest.mark.parametrize(
        ("open_options_attr", "use_explicit_open_options"),
        [
            pytest.param("MQOO_OUTPUT", False, id="default_output"),
            pytest.param(
                "MQOO_OUTPUT | MQOO_FAIL_IF_QUIESCING", True, id="custom_output_with_fail_if_quiescing"
            ),
        ],
    )
    @patch("ibmmq.connect")
    @patch("ibmmq.Queue")
    def test_produce_with_custom_open_options(
        self,
        mock_queue_class,
        mock_connect,
        mock_base_get_connection,
        open_options_attr,
        use_explicit_open_options,
    ):
        import ibmmq

        mock_qmgr = MagicMock()
        mock_connect.return_value = mock_qmgr

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        # Calculate the expected open_options value
        open_options_values = [getattr(ibmmq.CMQC, opt.strip()) for opt in open_options_attr.split("|")]
        expected_open_options = reduce(operator.or_, open_options_values)

        conn = mq_connection()
        if use_explicit_open_options:
            self.hook.produce(
                connection=conn,
                queue_name="QUEUE1",
                payload="test payload",
                open_options=expected_open_options,
            )
        else:
            self.hook.produce(connection=conn, queue_name="QUEUE1", payload="test payload")
            # When not specified, should default to MQOO_OUTPUT
            expected_open_options = ibmmq.CMQC.MQOO_OUTPUT

        mock_connect.assert_called_once()
        # Verify Queue was called with the expected open_options
        call_args = mock_queue_class.call_args
        actual_open_options = call_args[0][2]  # Third positional argument is open_options
        assert actual_open_options == expected_open_options
        mock_queue.put.assert_called_once()


class TestIBMMQConsumer:
    @pytest.fixture
    def event_loop(self):
        loop = asyncio.new_event_loop()
        yield loop
        loop.close()

    @pytest.fixture
    def mock_hook(self):
        hook = MagicMock(spec=IBMMQHook)
        hook.open_options = None
        return hook

    @pytest.fixture
    def stop_event(self):
        return threading.Event()

    @pytest.fixture
    def consumer(self, mock_hook, event_loop, stop_event):
        future = event_loop.create_future()
        return IBMMQConsumer(
            hook=mock_hook,
            connection=mq_connection(),
            queue_name="QUEUE1",
            poll_interval=0.1,
            loop=event_loop,
            future=future,
            stop_event=stop_event,
        )

    @patch("ibmmq.RFH2")
    def test_process_message_decodes_payload_after_rfh2_header(self, mock_rfh2_class, consumer):
        mock_rfh2 = MagicMock()
        mock_rfh2_class.return_value = mock_rfh2
        mock_rfh2.get_length.return_value = 10

        message = b"0123456789hello world"
        result = consumer._process_message(message)

        mock_rfh2.unpack.assert_called_once_with(message)
        assert result == "hello world"

    @patch("ibmmq.RFH2")
    def test_process_message_falls_back_to_raw_on_pyif_error(self, mock_rfh2_class, consumer):
        import ibmmq

        # Replace PYIFError with a plain Exception subclass so we can raise it in side_effect
        # while the except clause in _process_message still catches it.
        class FakePYIFError(Exception):
            pass

        mock_rfh2 = MagicMock()
        mock_rfh2_class.return_value = mock_rfh2
        mock_rfh2.unpack.side_effect = FakePYIFError("no rfh2 header")

        message = b"plain text message"
        with patch.object(ibmmq, "PYIFError", FakePYIFError):
            result = consumer._process_message(message)

        assert result == "plain text message"

    @pytest.mark.parametrize(
        ("exception_to_raise", "exception_name", "message_bytes", "expected_result", "log_contains"),
        [
            pytest.param(
                ValueError("invalid offset"),
                "ValueError",
                b"short message",
                "short message",
                "Failed to process RFH2 header (ValueError:",
                id="exception_from_get_length",
            ),
            pytest.param(
                Exception("generic error"),
                "Exception",
                b"malformed rfh2 data",
                "malformed rfh2 data",
                "Failed to process RFH2 header (Exception:",
                id="generic_exception_during_processing",
            ),
        ],
    )
    @patch("ibmmq.RFH2")
    def test_process_message_exception_fallback(
        self,
        mock_rfh2_class,
        consumer,
        caplog,
        exception_to_raise,
        exception_name,
        message_bytes,
        expected_result,
        log_contains,
    ):
        mock_rfh2 = MagicMock()
        mock_rfh2_class.return_value = mock_rfh2
        mock_rfh2.get_length.side_effect = exception_to_raise

        with caplog.at_level(logging.WARNING):
            result = consumer._process_message(message_bytes)

        assert result == expected_result
        assert log_contains in caplog.text
        assert "returning raw message" in caplog.text

    @patch("ibmmq.RFH2")
    def test_process_message_out_of_bounds_offset(self, mock_rfh2_class, consumer, caplog):
        mock_rfh2 = MagicMock()
        mock_rfh2_class.return_value = mock_rfh2
        mock_rfh2.get_length.return_value = 100  # Offset larger than message

        message = b"short message"
        with caplog.at_level(logging.WARNING):
            result = consumer._process_message(message)

        assert result == "short message"
        assert "RFH2 offset 100 exceeds message length 13; returning raw message" in caplog.text

    @patch("ibmmq.RFH2")
    def test_process_message_struct_error_falls_back(self, mock_rfh2_class, consumer, caplog):
        mock_rfh2 = MagicMock()
        mock_rfh2_class.return_value = mock_rfh2
        mock_rfh2.unpack.side_effect = struct.error("unpack requires a buffer of N bytes")

        message = b"malformed rfh2 data"
        with caplog.at_level(logging.WARNING):
            result = consumer._process_message(message)

        assert result == "malformed rfh2 data"
        assert "Failed to process RFH2 header (error:" in caplog.text
        assert "returning raw message" in caplog.text

    @pytest.mark.parametrize(
        ("log_level", "expect_debug_log"),
        [
            pytest.param(logging.DEBUG, True, id="debug_level_enabled"),
            pytest.param(logging.INFO, False, id="info_level_no_debug"),
        ],
    )
    @patch("ibmmq.RFH2")
    def test_process_message_debug_logging_payload(
        self, mock_rfh2_class, consumer, caplog, log_level, expect_debug_log
    ):
        mock_rfh2 = MagicMock()
        mock_rfh2_class.return_value = mock_rfh2
        mock_rfh2.get_length.return_value = 10

        message = b"0123456789hello world"
        with caplog.at_level(log_level):
            result = consumer._process_message(message)

        assert result == "hello world"
        if expect_debug_log:
            assert "Message received from MQ (RFH2 decoded):" in caplog.text
        else:
            assert "Message received from MQ (RFH2 decoded):" not in caplog.text

    @patch("ibmmq.RFH2")
    def test_process_message_payload_truncation(self, mock_rfh2_class, consumer, caplog):
        mock_rfh2 = MagicMock()
        mock_rfh2_class.return_value = mock_rfh2
        mock_rfh2.get_length.return_value = 10

        # Create a long payload (>200 chars)
        long_payload = "x" * 300
        message = b"0123456789" + long_payload.encode()
        with caplog.at_level(logging.DEBUG):
            result = consumer._process_message(message)

        assert result == long_payload
        # Check truncation marker
        debug_logs = [r.message for r in caplog.records if "Message received from MQ" in r.message]
        assert len(debug_logs) > 0
        assert "..." in debug_logs[0]  # Should have truncation marker

    @pytest.mark.parametrize(
        ("log_level", "expect_raw_payload_log"),
        [
            pytest.param(logging.DEBUG, True, id="fallback_debug_level_enabled"),
            pytest.param(logging.INFO, False, id="fallback_info_level_no_debug"),
        ],
    )
    @patch("ibmmq.RFH2")
    def test_process_message_fallback_debug_logging(
        self, mock_rfh2_class, consumer, caplog, log_level, expect_raw_payload_log
    ):
        mock_rfh2 = MagicMock()
        mock_rfh2_class.return_value = mock_rfh2
        mock_rfh2.unpack.side_effect = struct.error("unpack failed")

        message = b"error case message"
        with caplog.at_level(log_level):
            result = consumer._process_message(message)

        assert result == "error case message"
        # Warning should always appear
        assert "Failed to process RFH2 header" in caplog.text
        # Debug log should only appear at DEBUG level
        if expect_raw_payload_log:
            assert "Raw message payload (truncated):" in caplog.text
        else:
            assert "Raw message payload (truncated):" not in caplog.text

    @patch("ibmmq.Queue")
    @patch("ibmmq.GMO")
    @patch("ibmmq.MD")
    @patch("ibmmq.OD")
    def test_consume_returns_decoded_message_on_success(
        self, mock_od, mock_md, mock_gmo, mock_queue_class, consumer, mock_hook
    ):
        import ibmmq

        mock_conn = MagicMock()
        mock_hook.get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_hook.get_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_hook.get_open_options.return_value = ibmmq.CMQC.MQOO_INPUT_SHARED

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue
        mock_queue.get.return_value = b"hello from mq"

        with patch.object(consumer, "_process_message", return_value="hello from mq"):
            result = consumer.consume("QUEUE1", 0.1, threading.Event())

        assert result == "hello from mq"
        mock_queue.close.assert_called_once()

    @patch("ibmmq.Queue")
    @patch("ibmmq.GMO")
    @patch("ibmmq.MD")
    @patch("ibmmq.OD")
    def test_consume_returns_none_when_stop_event_set_before_loop(
        self, mock_od, mock_md, mock_gmo, mock_queue_class, consumer, mock_hook
    ):
        import ibmmq

        mock_conn = MagicMock()
        mock_hook.get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_hook.get_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_hook.get_open_options.return_value = ibmmq.CMQC.MQOO_INPUT_SHARED

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        stop_event = threading.Event()
        stop_event.set()

        result = consumer.consume("QUEUE1", 0.1, stop_event)

        assert result is None
        mock_queue.get.assert_not_called()
        mock_queue.close.assert_called_once()

    @patch("ibmmq.Queue")
    @patch("ibmmq.GMO")
    @patch("ibmmq.MD")
    @patch("ibmmq.OD")
    def test_consume_continues_on_no_msg_available_then_returns_none(
        self, mock_od, mock_md, mock_gmo, mock_queue_class, consumer, mock_hook
    ):
        import ibmmq

        mock_conn = MagicMock()
        mock_hook.get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_hook.get_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_hook.get_open_options.return_value = ibmmq.CMQC.MQOO_INPUT_SHARED

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        stop_event = threading.Event()

        def get_side_effect(*args, **kwargs):
            stop_event.set()
            raise ibmmq.MQMIError(comp=ibmmq.CMQC.MQCC_WARNING, reason=ibmmq.CMQC.MQRC_NO_MSG_AVAILABLE)

        mock_queue.get.side_effect = get_side_effect

        result = consumer.consume("QUEUE1", 0.1, stop_event)

        assert result is None
        mock_queue.get.assert_called_once()

    @patch("ibmmq.Queue")
    @patch("ibmmq.GMO")
    @patch("ibmmq.MD")
    @patch("ibmmq.OD")
    def test_consume_raises_on_unexpected_mq_error(
        self, mock_od, mock_md, mock_gmo, mock_queue_class, consumer, mock_hook
    ):
        import ibmmq

        mock_conn = MagicMock()
        mock_hook.get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_hook.get_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_hook.get_open_options.return_value = ibmmq.CMQC.MQOO_INPUT_SHARED

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue
        mock_queue.get.side_effect = ibmmq.MQMIError(
            comp=ibmmq.CMQC.MQCC_FAILED, reason=ibmmq.CMQC.MQRC_NOT_AUTHORIZED
        )

        with pytest.raises(IBMMQError) as exc_info:
            consumer.consume("QUEUE1", 0.1, threading.Event())

        assert exc_info.value.reason == ibmmq.CMQC.MQRC_NOT_AUTHORIZED
        assert exc_info.value.transient is False
        mock_queue.close.assert_called_once()

    @patch("ibmmq.Queue")
    @patch("ibmmq.GMO")
    @patch("ibmmq.MD")
    @patch("ibmmq.OD")
    def test_consume_closes_queue_even_when_exception_raised(
        self, mock_od, mock_md, mock_gmo, mock_queue_class, consumer, mock_hook
    ):
        import ibmmq

        mock_conn = MagicMock()
        mock_hook.get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_hook.get_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_hook.get_open_options.return_value = ibmmq.CMQC.MQOO_INPUT_SHARED

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue
        mock_queue.get.side_effect = ibmmq.MQMIError(
            comp=ibmmq.CMQC.MQCC_FAILED, reason=ibmmq.CMQC.MQRC_CONNECTION_BROKEN
        )

        with pytest.raises(IBMMQError) as exc_info:
            consumer.consume("QUEUE1", 0.1, threading.Event())

        assert exc_info.value.reason == ibmmq.CMQC.MQRC_CONNECTION_BROKEN
        assert exc_info.value.transient is True
        mock_queue.close.assert_called_once()

    @patch("ibmmq.GMO")
    @patch("ibmmq.MD")
    @patch("ibmmq.OD")
    @pytest.mark.parametrize(
        ("exc_type", "expected_transient"),
        [
            pytest.param("connection", True, id="connection_error_is_transient"),
            pytest.param("pyif", False, id="pyif_error_not_transient"),
        ],
    )
    def test_consume_wraps_non_mq_exceptions_as_ibmmq_error(
        self, mock_od, mock_md, mock_gmo, consumer, mock_hook, exc_type, expected_transient
    ):
        import ibmmq

        if exc_type == "connection":
            side_exc = ConnectionError("host unavailable")
        else:
            # ibmmq.PYIFError is a non-MQ exception from the C extension
            side_exc = ibmmq.PYIFError("pyif failure")

        mock_hook.get_conn.side_effect = side_exc

        with pytest.raises(IBMMQError) as exc_info:
            consumer.consume("QUEUE1", 0.1, threading.Event())

        assert exc_info.value.reason == _NON_MQ_SENTINEL
        assert exc_info.value.comp == _NON_MQ_SENTINEL
        assert exc_info.value.transient is expected_transient
        assert str(exc_info.value) == str(side_exc)

    @patch("ibmmq.Queue")
    @patch("ibmmq.GMO")
    @patch("ibmmq.MD")
    @patch("ibmmq.OD")
    def test_consume_close_suppresses_its_own_exception(
        self, mock_od, mock_md, mock_gmo, mock_queue_class, consumer, mock_hook
    ):
        import ibmmq

        mock_conn = MagicMock()
        mock_hook.get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_hook.get_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_hook.get_open_options.return_value = ibmmq.CMQC.MQOO_INPUT_SHARED

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue
        mock_queue.get.return_value = b"msg"
        mock_queue.close.side_effect = RuntimeError("close failed")

        with patch.object(consumer, "_process_message", return_value="msg"):
            # Should not raise even though close() fails
            result = consumer.consume("QUEUE1", 0.1, threading.Event())

        assert result == "msg"

    @patch("ibmmq.Queue")
    @patch("ibmmq.GMO")
    @patch("ibmmq.MD")
    @patch("ibmmq.OD")
    def test_consume_sets_gmo_wait_interval_from_poll_interval(
        self, mock_od_class, mock_md_class, mock_gmo_class, mock_queue_class, consumer, mock_hook
    ):
        import ibmmq

        mock_conn = MagicMock()
        mock_hook.get_conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_hook.get_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_hook.get_open_options.return_value = ibmmq.CMQC.MQOO_INPUT_SHARED

        mock_gmo = MagicMock()
        mock_gmo_class.return_value = mock_gmo

        mock_queue = MagicMock()
        mock_queue_class.return_value = mock_queue

        stop_event = threading.Event()
        stop_event.set()  # exit immediately

        consumer.consume("QUEUE1", 2.5, stop_event)

        assert mock_gmo.WaitInterval == 2500  # 2.5 s → 2500 ms

    @pytest.mark.parametrize(
        ("consume_outcome", "expected_result", "match"),
        [
            pytest.param("done", "done", None, id="success_result"),
            pytest.param(None, None, None, id="none_result"),
            pytest.param("mq_error", None, None, id="mq_error_exception"),
            pytest.param(
                TypeError("unexpected bug"), None, "unexpected bug", id="programming_error_exception"
            ),
        ],
    )
    def test_run(self, consumer, event_loop, consume_outcome, expected_result, match):
        if consume_outcome == "mq_error":
            import ibmmq

            consume_patch_kwargs = {
                "side_effect": IBMMQError(
                    reason=ibmmq.CMQC.MQRC_CONNECTION_BROKEN,
                    comp=ibmmq.CMQC.MQCC_FAILED,
                    transient=False,
                    message="Connection failed",
                )
            }
            resolved_exception = IBMMQError
        elif isinstance(consume_outcome, Exception):
            consume_patch_kwargs = {"side_effect": consume_outcome}
            resolved_exception = type(consume_outcome)
        else:
            consume_patch_kwargs = {"return_value": consume_outcome}
            resolved_exception = None

        with patch.object(consumer, "consume", **consume_patch_kwargs):
            consumer.run()
            event_loop.run_until_complete(asyncio.sleep(0))

        if resolved_exception is None:
            assert consumer.future.result() == expected_result
        elif match is None:
            with pytest.raises(resolved_exception):
                consumer.future.result()
        else:
            with pytest.raises(resolved_exception, match=match):
                consumer.future.result()

    @pytest.mark.parametrize(
        ("consume_outcome", "exception_msg"),
        [
            pytest.param("done", None, id="cancelled_after_success"),
            pytest.param(RuntimeError("test error"), "test error", id="cancelled_after_exception"),
        ],
    )
    def test_run_does_not_call_loop_when_future_cancelled(
        self, consumer, event_loop, consume_outcome, exception_msg
    ):
        consumer.future.cancel()

        if exception_msg:
            consume_kwargs = {"side_effect": consume_outcome}
        else:
            consume_kwargs = {"return_value": consume_outcome}

        with patch.object(consumer, "consume", **consume_kwargs):
            with patch.object(consumer.loop, "call_soon_threadsafe") as mock_call:
                consumer.run()
                event_loop.run_until_complete(asyncio.sleep(0))

        mock_call.assert_not_called()

    @pytest.mark.parametrize(
        ("consume_outcome", "expected_result", "expected_error"),
        [
            pytest.param("success", "success", None, id="sets_result_when_not_cancelled"),
            pytest.param(
                ValueError("test error"), None, "test error", id="sets_exception_when_not_cancelled"
            ),
        ],
    )
    def test_run_calls_loop_when_future_not_cancelled(
        self, consumer, event_loop, consume_outcome, expected_result, expected_error
    ):
        if isinstance(consume_outcome, Exception):
            consume_kwargs = {"side_effect": consume_outcome}
        else:
            consume_kwargs = {"return_value": consume_outcome}

        with patch.object(consumer, "consume", **consume_kwargs):
            with patch.object(
                consumer.loop, "call_soon_threadsafe", wraps=consumer.loop.call_soon_threadsafe
            ) as mock_call:
                consumer.run()
                event_loop.run_until_complete(asyncio.sleep(0))

        assert mock_call.called

        if expected_error is None:
            assert consumer.future.result() == expected_result
        else:
            with pytest.raises(ValueError, match=expected_error):
                consumer.future.result()

    def test_run_as_thread_completes_and_sets_result(self, event_loop):
        future = event_loop.create_future()
        stop_event = threading.Event()
        mock_hook = MagicMock(spec=IBMMQHook)

        consumer = IBMMQConsumer(
            hook=mock_hook,
            connection=mq_connection(),
            queue_name="QUEUE1",
            poll_interval=0.1,
            loop=event_loop,
            future=future,
            stop_event=stop_event,
        )
        with patch.object(consumer, "consume", return_value="threaded result"):
            consumer.start()
            consumer.join(timeout=5)

        event_loop.run_until_complete(asyncio.sleep(0))
        assert future.result() == "threaded result"
