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

from typing import TYPE_CHECKING, Sequence

import attr
import papermill as pm

from airflow.lineage.entities import File
from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


@attr.s(auto_attribs=True)
class NoteBook(File):
    """Jupyter notebook"""

    type_hint: str | None = "jupyter_notebook"
    parameters: dict | None = {}

    meta_schema: str = __name__ + '.NoteBook'


class PapermillOperator(BaseOperator):
    """
    Executes a jupyter notebook through papermill that is annotated with parameters

    :param input_nb: input notebook (can also be a NoteBook or a File inlet)
    :param output_nb: output notebook (can also be a NoteBook or File outlet)
    :param parameters: the notebook parameters to set
    :param kernel_name: (optional) name of kernel to execute the notebook against
        (ignores kernel name in the notebook document metadata)
    """

    supports_lineage = True

    template_fields: Sequence[str] = ('input_nb', 'output_nb', 'parameters', 'kernel_name', 'language_name')

    def __init__(
        self,
        *,
        input_nb: str | None = None,
        output_nb: str | None = None,
        parameters: dict | None = None,
        kernel_name: str | None = None,
        language_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.input_nb = input_nb
        self.output_nb = output_nb
        self.parameters = parameters
        self.kernel_name = kernel_name
        self.language_name = language_name
        if input_nb:
            self.inlets.append(NoteBook(url=input_nb, parameters=self.parameters))
        if output_nb:
            self.outlets.append(NoteBook(url=output_nb))

    def execute(self, context: Context):
        if not self.inlets or not self.outlets:
            raise ValueError("Input notebook or output notebook is not specified")

        for i, item in enumerate(self.inlets):
            pm.execute_notebook(
                item.url,
                self.outlets[i].url,
                parameters=item.parameters,
                progress_bar=False,
                report_mode=True,
                kernel_name=self.kernel_name,
                language=self.language_name,
            )
