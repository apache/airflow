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

from functools import cached_property
from typing import TYPE_CHECKING, ClassVar, Collection, Sequence

import attr
import papermill as pm

from airflow.lineage.entities import File
from airflow.models import BaseOperator
from airflow.providers.papermill.hooks.kernel import REMOTE_KERNEL_ENGINE, KernelHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


@attr.s(auto_attribs=True)
class NoteBook(File):
    """Jupyter notebook."""

    template_fields: ClassVar[Collection[str]] = {"parameters", *File.template_fields}

    type_hint: str | None = "jupyter_notebook"
    parameters: dict | None = {}

    meta_schema: str = __name__ + ".NoteBook"


class PapermillOperator(BaseOperator):
    """
    Executes a jupyter notebook through papermill that is annotated with parameters.

    :param input_nb: input notebook, either path or NoteBook inlet.
    :param output_nb: output notebook, either path or NoteBook outlet.
    :param parameters: the notebook parameters to set
    :param kernel_name: (optional) name of kernel to execute the notebook against
        (ignores kernel name in the notebook document metadata)
    """

    supports_lineage = True

    template_fields: Sequence[str] = (
        "input_nb",
        "output_nb",
        "parameters",
        "kernel_name",
        "language_name",
        "kernel_conn_id",
    )

    def __init__(
        self,
        *,
        input_nb: str | NoteBook | None = None,
        output_nb: str | NoteBook | None = None,
        parameters: dict | None = None,
        kernel_name: str | None = None,
        language_name: str | None = None,
        kernel_conn_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.parameters = parameters

        if not input_nb:
            raise ValueError("Input notebook is not specified")
        elif not isinstance(input_nb, NoteBook):
            self.input_nb = NoteBook(url=input_nb, parameters=self.parameters)
        else:
            self.input_nb = input_nb

        if not output_nb:
            raise ValueError("Output notebook is not specified")
        elif not isinstance(output_nb, NoteBook):
            self.output_nb = NoteBook(url=output_nb)
        else:
            self.output_nb = output_nb

        self.kernel_name = kernel_name
        self.language_name = language_name
        self.kernel_conn_id = kernel_conn_id

        self.inlets.append(self.input_nb)
        self.outlets.append(self.output_nb)

    def execute(self, context: Context):
        remote_kernel_kwargs = {}
        kernel_hook = self.hook
        if kernel_hook:
            engine_name = REMOTE_KERNEL_ENGINE
            kernel_connection = kernel_hook.get_conn()
            remote_kernel_kwargs = {
                "kernel_ip": kernel_connection.ip,
                "kernel_shell_port": kernel_connection.shell_port,
                "kernel_iopub_port": kernel_connection.iopub_port,
                "kernel_stdin_port": kernel_connection.stdin_port,
                "kernel_control_port": kernel_connection.control_port,
                "kernel_hb_port": kernel_connection.hb_port,
                "kernel_session_key": kernel_connection.session_key,
            }
        else:
            engine_name = None

        pm.execute_notebook(
            self.input_nb.url,
            self.output_nb.url,
            parameters=self.input_nb.parameters,
            progress_bar=False,
            report_mode=True,
            kernel_name=self.kernel_name,
            language=self.language_name,
            engine_name=engine_name,
            **remote_kernel_kwargs,
        )

    @cached_property
    def hook(self) -> KernelHook | None:
        """Get valid hook."""
        if self.kernel_conn_id:
            return KernelHook(kernel_conn_id=self.kernel_conn_id)
        else:
            return None
