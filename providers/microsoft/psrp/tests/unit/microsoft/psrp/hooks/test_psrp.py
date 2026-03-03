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

from logging import DEBUG, ERROR, INFO, WARNING
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from pypsrp.host import PSHost
from pypsrp.messages import MessageType
from pypsrp.powershell import PSInvocationState

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.microsoft.psrp.hooks.psrp import PsrpHook

CONNECTION_ID = "conn_id"
DUMMY_STACKTRACE = [
    r"at Invoke-Foo, C:\module.psm1: line 113",
    r"at Invoke-Bar, C:\module.psm1: line 125",
]


class MockPowerShell(MagicMock):
    had_errors = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state = PSInvocationState.NOT_STARTED

    def poll_invoke(self, timeout=None):
        self.state = PSInvocationState.COMPLETED
        self.output.append("output")

        def informational(message_type, message, **kwargs):
            kwargs.setdefault("command_name", "command")
            return Mock(MESSAGE_TYPE=message_type, message=message, **kwargs)

        self.streams.debug.append(informational(MessageType.DEBUG_RECORD, "debug1"))
        self.streams.debug.append(informational(MessageType.DEBUG_RECORD, "debug2\r\n", command_name=None))
        self.streams.verbose.append(informational(MessageType.VERBOSE_RECORD, "verbose"))
        self.streams.warning.append(informational(MessageType.WARNING_RECORD, "warning"))
        self.streams.information.append(
            Mock(
                MESSAGE_TYPE=MessageType.INFORMATION_RECORD,
                computer="computer",
                user="user",
                message_data="information",
            )
        )
        self.streams.progress.append(
            Mock(MESSAGE_TYPE=MessageType.PROGRESS_RECORD, activity="activity", description="description")
        )

        if self.had_errors:
            self.streams.error.append(
                Mock(
                    MESSAGE_TYPE=MessageType.ERROR_RECORD,
                    command_name="command",
                    message="error",
                    reason="reason",
                    script_stacktrace="\r\n".join(DUMMY_STACKTRACE),
                )
            )

    def begin_invoke(self):
        self.state = PSInvocationState.RUNNING
        self.output = []
        self.streams.debug = []
        self.streams.error = []
        self.streams.information = []
        self.streams.progress = []
        self.streams.verbose = []
        self.streams.warning = []

    def end_invoke(self):
        while self.state == PSInvocationState.RUNNING:
            self.poll_invoke()


def mock_powershell_factory():
    return MagicMock(return_value=MockPowerShell())


@patch(
    f"{PsrpHook.__module__}.{PsrpHook.__name__}.get_connection",
    new=lambda _, conn_id: Connection(
        conn_id=conn_id,
        login="username",
        password="password",
        host="remote_host",
    ),
)
@patch(f"{PsrpHook.__module__}.WSMan")
@patch(f"{PsrpHook.__module__}.PowerShell", new_callable=mock_powershell_factory)
@patch(f"{PsrpHook.__module__}.RunspacePool")
class TestPsrpHook:
    def test_get_conn(self, runspace_pool, powershell, ws_man):
        hook = PsrpHook(CONNECTION_ID)
        assert hook.get_conn() is runspace_pool.return_value

    def test_get_conn_unexpected_extra(self, runspace_pool, powershell, ws_man):
        hook = PsrpHook(CONNECTION_ID)
        conn = hook.get_connection(CONNECTION_ID)

        def get_connection(*args):
            conn.extra = '{"foo": "bar"}'
            return conn

        hook.get_connection = get_connection
        with pytest.raises(AirflowException, match="Unexpected extra configuration keys: foo"):
            hook.get_conn()

    @pytest.mark.parametrize(
        "logging_level", [pytest.param(None, id="none"), pytest.param(ERROR, id="ERROR")]
    )
    def test_invoke(self, runspace_pool, powershell, ws_man, logging_level):
        runspace_options = {"connection_name": "foo"}
        wsman_options = {"encryption": "auto"}

        options = {}
        if logging_level is not None:
            options["logging_level"] = logging_level

        on_output_callback = Mock()

        with (
            PsrpHook(
                CONNECTION_ID,
                runspace_options=runspace_options,
                wsman_options=wsman_options,
                on_output_callback=on_output_callback,
                **options,
            ) as hook,
            patch.object(type(hook), "log") as logger,
        ):
            error_match = "Process had one or more errors"
            with pytest.raises(AirflowException, match=error_match):  # noqa: PT012 error happen on context exit
                with hook.invoke() as ps:
                    assert ps.state == PSInvocationState.NOT_STARTED

                    # We're simulating an error in order to test error
                    # handling as well as the logging of error exception
                    # details.
                    ps.had_errors = True
            assert ps.state == PSInvocationState.COMPLETED

        assert on_output_callback.mock_calls == [call("output")]
        assert runspace_pool.return_value.__exit__.mock_calls == [call(None, None, None)]
        assert ws_man().__exit__.mock_calls == [call(None, None, None)]
        assert ws_man.call_args_list[0][1]["encryption"] == "auto"
        assert logger.method_calls[0] == call.setLevel(logging_level or DEBUG)

        def assert_log(level, *args):
            assert call.log(level, *args) in logger.method_calls

        assert_log(DEBUG, "%s: %s", "command", "debug1")
        assert_log(DEBUG, "%s", "debug2")
        assert_log(ERROR, "%s: %s", "command", "error")
        assert_log(INFO, "%s: %s", "command", "verbose")
        assert_log(WARNING, "%s: %s", "command", "warning")
        assert_log(INFO, "Progress: %s (%s)", "activity", "description")
        assert_log(INFO, "%s (%s): %s", "computer", "user", "information")
        assert_log(INFO, "%s: %s", "reason", ps.streams.error[0])
        assert_log(INFO, DUMMY_STACKTRACE[0])
        assert_log(INFO, DUMMY_STACKTRACE[1])

        assert call("Invocation state: %s", "Completed") in logger.info.mock_calls
        args, kwargs = runspace_pool.call_args
        assert args == (ws_man.return_value,)
        assert kwargs["connection_name"] == "foo"
        assert isinstance(kwargs["host"], PSHost)

    def test_invoke_cmdlet(self, *mocks):
        arguments = ("a", "b", "c")
        parameters = {"bar": "1", "baz": "2"}
        with PsrpHook(CONNECTION_ID) as hook:
            ps = hook.invoke_cmdlet("foo", arguments=arguments, parameters=parameters)
            assert [call("foo", use_local_scope=None)] == ps.add_cmdlet.mock_calls
            assert [call({"bar": "1", "baz": "2"})] == ps.add_parameters.mock_calls
            assert [call(arg) for arg in arguments] == ps.add_argument.mock_calls

    def test_invoke_powershell(self, *mocks):
        with PsrpHook(CONNECTION_ID) as hook:
            ps = hook.invoke_powershell("foo")
            assert call("foo") in ps.add_script.mock_calls

    def test_invoke_local_context(self, *mocks):
        hook = PsrpHook(CONNECTION_ID)
        ps = hook.invoke_powershell("foo")
        assert call("foo") in ps.add_script.mock_calls

    def test_test_connection(self, runspace_pool, *mocks):
        connection = Connection(conn_type="psrp")
        connection.test_connection()

        assert runspace_pool.return_value.__enter__.mock_calls == [call()]
