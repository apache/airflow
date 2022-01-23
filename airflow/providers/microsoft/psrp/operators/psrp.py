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

from typing import TYPE_CHECKING, List, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.psrp.hooks.psrp import PSRPHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class PSRPOperator(BaseOperator):
    """PowerShell Remoting Protocol operator.

    :param psrp_conn_id: connection id
    :param command: command to execute on remote host. (templated)
    :param powershell: powershell to execute on remote host. (templated)
    """

    template_fields: Sequence[str] = (
        "command",
        "powershell",
    )
    template_fields_renderers = {"command": "powershell", "powershell": "powershell"}
    ui_color = "#901dd2"

    def __init__(
        self,
        *,
        psrp_conn_id: str,
        command: Optional[str] = None,
        powershell: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if not (command or powershell):
            raise ValueError("Must provide either 'command' or 'powershell'")
        self.conn_id = psrp_conn_id
        self.command = command
        self.powershell = powershell

    def execute(self, context: "Context") -> List[str]:
        with PSRPHook(self.conn_id) as hook:
            ps = hook.invoke_powershell(
                f"cmd.exe /c @'\n{self.command}\n'@" if self.command else self.powershell
            )
        if ps.had_errors:
            raise AirflowException("Process failed")
        return ps.output
