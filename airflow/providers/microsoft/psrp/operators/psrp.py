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

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.psrp.hooks.psrp import PSRPHook
from airflow.utils.helpers import exactly_one

if TYPE_CHECKING:
    from airflow.utils.context import Context


class PSRPOperator(BaseOperator):
    """PowerShell Remoting Protocol operator.

    :param psrp_conn_id: connection id
    :param command: command to execute on remote host. (templated)
    :param powershell: powershell to execute on remote host. (templated)
    :param cmdlet: cmdlet to execute on remote host. (templated)
    :param parameters: parameters to provide to cmdlet. (templated)
    :param logging: whether to log command output and streams during execution
    :param runspace_options:
        Optional dictionary which is passed when creating the runspace pool. See
        :py:class:`~pypsrp.powershell.RunspacePool` for a description of the
        available options.
    """

    template_fields: Sequence[str] = (
        "command",
        "powershell",
        "cmdlet",
    )
    template_fields_renderers = {"command": "powershell", "powershell": "powershell"}
    ui_color = "#901dd2"

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
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        args = {command, powershell, cmdlet}
        if not exactly_one(*args):
            raise ValueError("Must provide exactly one of 'command', 'powershell', or 'cmdlet'")
        if parameters and not cmdlet:
            raise ValueError("Parameters only allowed with 'cmdlet'")
        self.conn_id = psrp_conn_id
        self.command = command
        self.powershell = powershell
        self.cmdlet = cmdlet
        self.parameters = parameters or {}
        self.logging = logging
        self.runspace_options = runspace_options

    def execute(self, context: "Context") -> List[str]:
        with PSRPHook(self.conn_id, logging=self.logging, runspace_options=self.runspace_options) as hook:
            ps = (
                hook.invoke_cmdlet(self.cmdlet, **self.parameters)
                if self.cmdlet
                else (
                    hook.invoke_powershell(
                        f"cmd.exe /c @'\n{self.command}\n'@" if self.command else self.powershell
                    )
                )
            )
        if ps.had_errors:
            raise AirflowException("Process failed")
        return ps.output
