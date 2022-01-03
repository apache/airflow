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

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

from jinja2.nativetypes import NativeEnvironment
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

    The command output is converted to JSON by PowerShell such that the operator
    return value is serializable to an XCom value.

    :param psrp_conn_id: connection id
    :param command: command to execute on remote host. (templated)
    :param powershell: powershell to execute on remote host. (templated)
    :param cmdlet:
        cmdlet to execute on remote host (templated). Also used as the default
        value for `task_id`.
    :param parameters:
        parameters to provide to cmdlet (templated). This is allowed only if
        the `cmdlet` parameter is also given.
    :param logging: whether to log command output and streams during execution
    :param runspace_options:
        optional dictionary which is passed when creating the runspace pool. See
        :py:class:`~pypsrp.powershell.RunspacePool` for a description of the
        available options.
    :param wsman_options:
        optional dictionary which is passed when creating the `WSMan` client. See
        :py:class:`~pypsrp.wsman.WSMan` for a description of the available options.
    """

    template_fields: Sequence[str] = (
        "cmdlet",
        "command",
        "parameters",
        "powershell",
    )
    template_fields_renderers = {"command": "powershell", "powershell": "powershell"}
    ui_color = "#c2e2ff"

    def __init__(
        self,
        *,
        psrp_conn_id: str,
        command: Optional[str] = None,
        powershell: Optional[str] = None,
        cmdlet: Optional[str] = None,
        parameters: Optional[Dict[str, str]] = None,
        logging: bool = True,
        runspace_options: Optional[Dict[str, Any]] = None,
        wsman_options: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        args = {command, powershell, cmdlet}
        if not exactly_one(*args):
            raise ValueError("Must provide exactly one of 'command', 'powershell', or 'cmdlet'")
        if parameters and not cmdlet:
            raise ValueError("Parameters only allowed with 'cmdlet'")
        if cmdlet:
            kwargs.setdefault('task_id', cmdlet)
        super().__init__(**kwargs)
        self.conn_id = psrp_conn_id
        self.command = command
        self.powershell = powershell
        self.cmdlet = cmdlet
        self.parameters = parameters or {}
        self.logging = logging
        self.runspace_options = runspace_options
        self.wsman_options = wsman_options

    def execute(self, context: "Context") -> List[Any]:
        with PsrpHook(
            self.conn_id,
            logging=self.logging,
            runspace_options=self.runspace_options,
            wsman_options=self.wsman_options,
        ) as hook, hook.invoke() as ps:
            if self.cmdlet:
                ps.add_cmdlet(self.cmdlet)
                ps.add_parameters(self.parameters)
            elif self.command:
                ps.add_script(f"cmd.exe /c @'\n{self.command}\n'@")
            else:
                ps.add_script(self.powershell)
            ps.add_cmdlet("ConvertTo-Json")

        if ps.had_errors:
            raise AirflowException("Process failed")

        return [json.loads(output) for output in ps.output]

    def get_template_env(self):
        # Create a template environment overlay in order to leave the underlying
        # environment unchanged.
        env = super().get_template_env().overlay()
        native = isinstance(env, NativeEnvironment)

        def securestring(value: str):
            if not native:
                raise AirflowException(
                    "Filter 'securestring' not applicable to non-native " "templating environment"
                )
            return TaggedValue("SS", value)

        env.filters["securestring"] = securestring

        return env
