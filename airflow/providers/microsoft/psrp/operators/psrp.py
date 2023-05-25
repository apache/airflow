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

from logging import DEBUG
from typing import TYPE_CHECKING, Any, Sequence

from jinja2.nativetypes import NativeEnvironment
from pypsrp.powershell import Command
from pypsrp.serializer import TaggedValue

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.psrp.hooks.psrp import PsrpHook
from airflow.settings import json
from airflow.utils.helpers import exactly_one

if TYPE_CHECKING:
    from airflow.utils.context import Context


class PsrpOperator(BaseOperator):
    """PowerShell Remoting Protocol operator.

    Use one of the 'command', 'cmdlet', or 'powershell' arguments.

    The 'securestring' template filter can be used to tag a value for
    serialization into a `System.Security.SecureString` (applicable only
    for DAGs which have `render_template_as_native_obj=True`).

    When using the `cmdlet` or `powershell` arguments and when `do_xcom_push`
    is enabled, the command output is converted to JSON by PowerShell using
    the `ConvertTo-Json
    <https://docs.microsoft.com/en-us/powershell/
    module/microsoft.powershell.utility/convertto-json>`__ cmdlet such
    that the operator return value is serializable to an XCom value.

    :param psrp_conn_id: connection id
    :param command: command to execute on remote host. (templated)
    :param powershell: powershell to execute on remote host. (templated)
    :param cmdlet:
        cmdlet to execute on remote host (templated). Also used as the default
        value for `task_id`.
    :param arguments:
        When using the `cmdlet` or `powershell` option, use `arguments` to
        provide arguments (templated).
    :param parameters:
        When using the `cmdlet` or `powershell` option, use `parameters` to
        provide parameters (templated). Note that a parameter with a value of `None`
        becomes an *argument* (i.e., switch).
    :param logging_level:
        Logging level for message streams which are received during remote execution.
        The default is to include all messages in the task log.
    :param runspace_options:
        optional dictionary which is passed when creating the runspace pool. See
        :py:class:`~pypsrp.powershell.RunspacePool` for a description of the
        available options.
    :param wsman_options:
        optional dictionary which is passed when creating the `WSMan` client. See
        :py:class:`~pypsrp.wsman.WSMan` for a description of the available options.
    :param psrp_session_init:
        Optional command which will be added to the pipeline when a new PowerShell
        session has been established, prior to invoking the action specified using
        the `cmdlet`, `command`, or `powershell` parameters.
    """

    template_fields: Sequence[str] = (
        "cmdlet",
        "command",
        "arguments",
        "parameters",
        "powershell",
    )
    template_fields_renderers = {"command": "powershell", "powershell": "powershell"}
    ui_color = "#c2e2ff"

    def __init__(
        self,
        *,
        psrp_conn_id: str,
        command: str | None = None,
        powershell: str | None = None,
        cmdlet: str | None = None,
        arguments: list[str] | None = None,
        parameters: dict[str, str] | None = None,
        logging_level: int = DEBUG,
        runspace_options: dict[str, Any] | None = None,
        wsman_options: dict[str, Any] | None = None,
        psrp_session_init: Command | None = None,
        **kwargs,
    ) -> None:
        args = {command, powershell, cmdlet}
        if not exactly_one(*args):
            raise ValueError("Must provide exactly one of 'command', 'powershell', or 'cmdlet'")
        if arguments and not cmdlet:
            raise ValueError("Arguments only allowed with 'cmdlet'")
        if parameters and not cmdlet:
            raise ValueError("Parameters only allowed with 'cmdlet'")
        if cmdlet:
            kwargs.setdefault("task_id", cmdlet)
        super().__init__(**kwargs)
        self.conn_id = psrp_conn_id
        self.command = command
        self.powershell = powershell
        self.cmdlet = cmdlet
        self.arguments = arguments
        self.parameters = parameters
        self.logging_level = logging_level
        self.runspace_options = runspace_options
        self.wsman_options = wsman_options
        self.psrp_session_init = psrp_session_init

    def execute(self, context: Context) -> list[Any] | None:
        with PsrpHook(
            self.conn_id,
            logging_level=self.logging_level,
            runspace_options=self.runspace_options,
            wsman_options=self.wsman_options,
            on_output_callback=self.log.info if not self.do_xcom_push else None,
        ) as hook, hook.invoke() as ps:
            if self.psrp_session_init is not None:
                ps.add_command(self.psrp_session_init)
            if self.command:
                ps.add_script(f"cmd.exe /c @'\n{self.command}\n'@")
            else:
                if self.cmdlet:
                    ps.add_cmdlet(self.cmdlet)
                else:
                    ps.add_script(self.powershell)
                for argument in self.arguments or ():
                    ps.add_argument(argument)
                if self.parameters:
                    ps.add_parameters(self.parameters)
                if self.do_xcom_push:
                    ps.add_cmdlet("ConvertTo-Json")

        if ps.had_errors:
            raise AirflowException("Process failed")

        rc = ps.runspace_pool.host.rc
        if rc:
            raise AirflowException(f"Process exited with non-zero status code: {rc}")

        if not self.do_xcom_push:
            return None

        return [json.loads(output) for output in ps.output]

    def get_template_env(self):
        # Create a template environment overlay in order to leave the underlying
        # environment unchanged.
        env = super().get_template_env().overlay()
        native = isinstance(env, NativeEnvironment)

        def securestring(value: str):
            if not native:
                raise AirflowException(
                    "Filter 'securestring' not applicable to non-native templating environment"
                )
            return TaggedValue("SS", value)

        env.filters["securestring"] = securestring

        return env
