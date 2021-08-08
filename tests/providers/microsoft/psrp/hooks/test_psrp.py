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

import unittest
from unittest.mock import MagicMock, call, patch

from pypsrp.messages import InformationRecord
from pypsrp.powershell import PSInvocationState

from airflow.models import Connection
from airflow.providers.microsoft.psrp.hooks.psrp import PSRPHook

CONNECTION_ID = "conn_id"


class TestPSRPHook(unittest.TestCase):
    @patch(
        f"{PSRPHook.__module__}.{PSRPHook.__name__}.get_connection",
        return_value=Connection(
            login='username',
            password='password',
            host='remote_host',
        ),
    )
    @patch(f"{PSRPHook.__module__}.WSMan")
    @patch(f"{PSRPHook.__module__}.PowerShell")
    @patch(f"{PSRPHook.__module__}.RunspacePool")
    @patch("logging.Logger.info")
    def test_invoke_powershell(self, log_info, runspace_pool, powershell, ws_man, get_connection):
        with PSRPHook(CONNECTION_ID) as hook:
            ps = powershell.return_value = MagicMock()
            ps.state = PSInvocationState.RUNNING
            ps.output = []
            ps.streams.debug = []
            ps.streams.information = []
            ps.streams.error = []

            def poll_invoke():
                ps.output.append("<output>")
                ps.streams.debug.append(MagicMock(spec=InformationRecord, message_data="<message>"))
                ps.state = PSInvocationState.COMPLETED

            def end_invoke():
                ps.streams.error = []

            ps.poll_invoke.side_effect = poll_invoke
            ps.end_invoke.side_effect = end_invoke

            hook.invoke_powershell("foo")

        assert call('%s', '<output>') in log_info.mock_calls
        assert call('Information: %s', '<message>') in log_info.mock_calls
        assert call('Invocation state: %s', 'Completed') in log_info.mock_calls
