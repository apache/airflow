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

from typing import Any, NamedTuple
from unittest.mock import Mock, call, patch

import pytest
from jinja2.nativetypes import NativeEnvironment
from pypsrp.powershell import Command, PowerShell

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.microsoft.psrp.operators.psrp import PsrpOperator
from airflow.settings import json

CONNECTION_ID = "conn_id"


class ExecuteParameter(NamedTuple):
    name: str
    expected_method: str
    expected_arguments: list[str] | None
    expected_parameters: dict[str, Any] | None


class TestPsrpOperator:
    def test_no_command_or_powershell(self):
        exception_msg = "Must provide exactly one of 'command', 'powershell', or 'cmdlet'"
        with pytest.raises(ValueError, match=exception_msg):
            PsrpOperator(task_id="test_task_id", psrp_conn_id=CONNECTION_ID)

    def test_cmdlet_task_id_default(self):
        operator = PsrpOperator(cmdlet="Invoke-Foo", psrp_conn_id=CONNECTION_ID)
        assert operator.task_id == "Invoke-Foo"

    @pytest.mark.parametrize("do_xcom_push", [True, False])
    @pytest.mark.parametrize(
        "had_errors, rc", [(False, 0), (False, None), (True, None), (False, 1), (True, 1)]
    )
    @pytest.mark.parametrize(
        "parameter",
        [
            # These tuples map the command parameter to an execution method and parameter set.
            pytest.param(
                ExecuteParameter("command", call.add_script("cmd.exe /c @'\nfoo\n'@"), None, None),
                id="command",
            ),
            pytest.param(ExecuteParameter("powershell", call.add_script("foo"), None, None), id="powershell"),
            pytest.param(
                ExecuteParameter("cmdlet", call.add_cmdlet("foo"), ["abc"], {"bar": "baz"}), id="cmdlet"
            ),
        ],
    )
    @patch(f"{PsrpOperator.__module__}.PsrpHook")
    def test_execute(self, hook_impl, parameter, had_errors, rc, do_xcom_push):
        kwargs = {parameter.name: "foo"}
        if parameter.expected_arguments:
            kwargs["arguments"] = parameter.expected_arguments
        if parameter.expected_parameters:
            kwargs["parameters"] = parameter.expected_parameters
        psrp_session_init = Mock(spec=Command)
        op = PsrpOperator(
            task_id="test_task_id",
            psrp_conn_id=CONNECTION_ID,
            psrp_session_init=psrp_session_init,
            do_xcom_push=do_xcom_push,
            **kwargs,
        )
        runspace_pool = Mock()
        runspace_pool.host.rc = rc
        ps = Mock(
            spec=PowerShell,
            output=[json.dumps("<output>")],
            had_errors=had_errors,
            runspace_pool=runspace_pool,
        )
        hook_impl.configure_mock(
            **{"return_value.__enter__.return_value.invoke.return_value.__enter__.return_value": ps}
        )
        if had_errors or rc:
            exception_msg = "Process failed" if had_errors else "Process exited with non-zero status code: 1"
            with pytest.raises(AirflowException, match=exception_msg):
                op.execute(None)
        else:
            output = op.execute(None)
            assert output == [json.loads(output) for output in ps.output] if do_xcom_push else ps.output
            is_logged = hook_impl.call_args.kwargs["on_output_callback"] == op.log.info
            assert do_xcom_push ^ is_logged
        expected_ps_calls = [
            call.add_command(psrp_session_init),
            parameter.expected_method,
        ]
        if parameter.expected_arguments:
            expected_ps_calls.append(call.add_argument("abc"))
        if parameter.expected_parameters:
            expected_ps_calls.extend([call.add_parameters({"bar": "baz"})])
        if parameter.name in ("cmdlet", "powershell") and do_xcom_push:
            expected_ps_calls.append(
                call.add_cmdlet("ConvertTo-Json"),
            )
        assert ps.mock_calls == expected_ps_calls

    def test_securestring_sandboxed(self):
        op = PsrpOperator(psrp_conn_id=CONNECTION_ID, cmdlet="test")
        template = op.get_template_env().from_string("{{ 'foo' | securestring }}")
        with pytest.raises(AirflowException):
            template.render()

    @patch.object(BaseOperator, "get_template_env")
    def test_securestring_native(self, get_template_env):
        op = PsrpOperator(psrp_conn_id=CONNECTION_ID, cmdlet="test")
        get_template_env.return_value = NativeEnvironment()
        template = op.get_template_env().from_string("{{ 'foo' | securestring }}")
        rendered = template.render()
        assert rendered.tag == "SS"
        assert rendered.value == "foo"
