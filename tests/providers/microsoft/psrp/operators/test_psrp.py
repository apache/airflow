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

from itertools import product
from unittest import TestCase
from unittest.mock import call, patch

import pytest
from jinja2.nativetypes import NativeEnvironment
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.microsoft.psrp.operators.psrp import PSRPOperator

CONNECTION_ID = "conn_id"


class TestPSRPOperator(TestCase):
    def test_no_command_or_powershell(self):
        exception_msg = "Must provide exactly one of 'command', 'powershell', or 'cmdlet'"
        with pytest.raises(ValueError, match=exception_msg):
            PSRPOperator(task_id='test_task_id', psrp_conn_id=CONNECTION_ID)

    def test_cmdlet_task_id_default(self):
        operator = PSRPOperator(cmdlet='Invoke-Foo', psrp_conn_id=CONNECTION_ID)
        assert operator.task_id == 'Invoke-Foo'

    @parameterized.expand(
        list(
            product(
                [
                    # These tuples map the command parameter to an execution method
                    # and parameter set.
                    ("command", "powershell", None),
                    ("powershell", "powershell", None),
                    ("cmdlet", "cmdlet", {"bar": "baz"}),
                ],
                [False, True],
            )
        )
    )
    @patch(f"{PSRPOperator.__module__}.PSRPHook")
    def test_execute(self, parameter, had_errors, hook):
        kwargs = {parameter[0]: "foo"}
        if parameter[2]:
            kwargs["parameters"] = parameter[2]
        op = PSRPOperator(task_id='test_task_id', psrp_conn_id=CONNECTION_ID, **kwargs)
        hook = hook.return_value.__enter__.return_value
        method = getattr(hook, f"invoke_{parameter[1]}")
        ps = method.return_value
        ps.output = ["<output>"]
        ps.had_errors = had_errors
        if had_errors:
            exception_msg = "Process failed"
            with pytest.raises(AirflowException, match=exception_msg):
                op.execute(None)
        else:
            output = op.execute(None)
            assert output == ps.output
        if parameter[2]:
            assert method.mock_calls == [call('foo', bar='baz')]

    def test_securestring_sandboxed(self):
        op = PSRPOperator(psrp_conn_id=CONNECTION_ID, cmdlet='test')
        template = op.get_template_env().from_string("{{ 'foo' | securestring }}")
        with pytest.raises(AirflowException):
            template.render()

    @patch.object(BaseOperator, "get_template_env")
    def test_securestring_native(self, get_template_env):
        op = PSRPOperator(psrp_conn_id=CONNECTION_ID, cmdlet='test')
        get_template_env.return_value = NativeEnvironment()
        template = op.get_template_env().from_string("{{ 'foo' | securestring }}")
        rendered = template.render()
        assert rendered.tag == "SS"
        assert rendered.value == "foo"
