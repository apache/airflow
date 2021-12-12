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
#

from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from parameterized import parameterized
from pypsrp.messages import (
    DebugRecord,
    ErrorRecord,
    InformationRecord,
    ProgressRecord,
    VerboseRecord,
    WarningRecord,
)
from pypsrp.powershell import PSInvocationState
from pytest import raises

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.psrp.hooks.psrp import PSRPHook

CONNECTION_ID = "conn_id"


class MockPowerShell(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state = PSInvocationState.NOT_STARTED

    def poll_invoke(self, timeout=None):
        self.output.append("output")

        def record(spec, message):
            return MagicMock(spec=spec, command_name="command", message=message)

        self.streams.debug.append(record(DebugRecord, "debug"))
        self.streams.error.append(record(ErrorRecord, "error"))
        self.streams.verbose.append(record(VerboseRecord, "verbose"))
        self.streams.warning.append(record(WarningRecord, "warning"))
        self.streams.information.append(
            MagicMock(spec=InformationRecord, computer="computer", user="user", message_data="information")
        )
        self.streams.progress.append(
            MagicMock(spec=ProgressRecord, activity="activity", description="description")
        )
        self.state = PSInvocationState.COMPLETED

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
        self.streams.error = []


def mock_powershell_factory():
    return MagicMock(return_value=MockPowerShell())


@patch(
    f"{PSRPHook.__module__}.{PSRPHook.__name__}.get_connection",
    new=lambda _, conn_id: Connection(
        conn_id=conn_id,
        login='username',
        password='password',
        host='remote_host',
    ),
)
@patch(f"{PSRPHook.__module__}.WSMan")
@patch(f"{PSRPHook.__module__}.PowerShell", new_callable=mock_powershell_factory)
@patch(f"{PSRPHook.__module__}.RunspacePool")
class TestPSRPHook(TestCase):
    def test_get_conn(self, runspace_pool, powershell, ws_man):
        hook = PSRPHook(CONNECTION_ID)
        assert hook.get_conn() is runspace_pool.return_value

    def test_get_conn_unexpected_extra(self, runspace_pool, powershell, ws_man):
        hook = PSRPHook(CONNECTION_ID)
        conn = hook.get_connection(CONNECTION_ID)

        def get_connection(*args):
            conn.extra = '{"foo": "bar"}'
            return conn

        hook.get_connection = get_connection
        with raises(AirflowException, match="Unexpected extra configuration keys: foo"):
            hook.get_conn()

    @parameterized.expand([(False,), (True,)])
    @patch("logging.Logger.warning")
    @patch("logging.Logger.info")
    @patch("logging.Logger.error")
    @patch("logging.Logger.debug")
    def test_invoke(
        self, logging, log_debug, log_error, log_info, log_warning, runspace_pool, powershell, ws_man
    ):
        runspace_options = {"connection_name": "foo"}
        wsman_options = {"encryption": "auto"}
        with PSRPHook(
            CONNECTION_ID, logging=logging, runspace_options=runspace_options, wsman_options=wsman_options
        ) as hook:
            with hook.invoke() as ps:
                assert ps.state == PSInvocationState.NOT_STARTED
            assert ps.state == PSInvocationState.COMPLETED

            # Since we've entered into the hook context, the
            # `get_conn` method returns the active runspace pool.
            assert hook.get_conn() is runspace_pool.return_value

        assert runspace_pool.return_value.__exit__.mock_calls == [call(None, None, None)]
        assert ws_man().__exit__.mock_calls == [call(None, None, None)]
        assert ws_man.call_args_list[0][1]["encryption"] == "auto"

        def assert_log(f, *args):
            assert not (logging ^ (call(*args) in f.mock_calls))

        assert_log(log_debug, '%s: %s', 'command', 'debug')
        assert_log(log_error, '%s: %s', 'command', 'error')
        assert_log(log_info, '%s: %s', 'command', 'verbose')
        assert_log(log_warning, '%s: %s', 'command', 'warning')
        assert_log(log_info, 'Progress: %s (%s)', 'activity', 'description')
        assert_log(log_info, '%s (%s): %s', 'computer', 'user', 'information')

        assert call('Invocation state: %s', 'Completed') in log_info.mock_calls
        assert runspace_pool.call_args == call(ws_man.return_value, connection_name='foo')

    def test_invoke_cmdlet(self, *mocks):
        with PSRPHook(CONNECTION_ID, logging=False) as hook:
            ps = hook.invoke_cmdlet('foo', bar="1", baz="2")
            assert [call('foo', use_local_scope=None)] == ps.add_cmdlet.mock_calls
            assert [call({'bar': '1', 'baz': '2'})] == ps.add_parameters.mock_calls

    def test_invoke_powershell(self, *mocks):
        with PSRPHook(CONNECTION_ID, logging=False) as hook:
            ps = hook.invoke_powershell('foo')
            assert call('foo') in ps.add_script.mock_calls

    def test_invoke_local_context(self, *mocks):
        hook = PSRPHook(CONNECTION_ID, logging=False)
        ps = hook.invoke_powershell('foo')
        assert call('foo') in ps.add_script.mock_calls
