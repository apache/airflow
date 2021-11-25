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
from pypsrp.messages import InformationRecord
from pypsrp.powershell import PSInvocationState

from airflow.models import Connection
from airflow.providers.microsoft.psrp.hooks.psrp import PSRPHook

CONNECTION_ID = "conn_id"


class MockPowerShell(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state = PSInvocationState.NOT_STARTED

    def poll_invoke(self, timeout=None):
        self.output.append("<output>")
        self.streams.debug.append(MagicMock(spec=InformationRecord, message_data="<message>"))
        self.state = PSInvocationState.COMPLETED

    def begin_invoke(self):
        self.state = PSInvocationState.RUNNING
        self.output = []
        self.streams.debug = []
        self.streams.information = []
        self.streams.error = []

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
@patch("logging.Logger.info")
class TestPSRPHook(TestCase):
    @parameterized.expand([(False,), (True,)])
    def test_invoke(self, log_info, runspace_pool, powershell, ws_man, logging):
        runspace_options = {"connection_name": "foo"}
        with PSRPHook(CONNECTION_ID, logging=logging, runspace_options=runspace_options) as hook:
            with hook.invoke() as ps:
                assert ps.state == PSInvocationState.NOT_STARTED
            assert ps.state == PSInvocationState.COMPLETED

        assert ws_man().__exit__.mock_calls == [call(None, None, None)]
        assert not (logging ^ (call('%s', '<output>') in log_info.mock_calls))
        assert not (logging ^ (call('Information: %s', '<message>') in log_info.mock_calls))
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
