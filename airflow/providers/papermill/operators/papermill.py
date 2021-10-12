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
from typing import Dict, Optional

import attr
import papermill as pm

from airflow.lineage.entities import File
from airflow.models import BaseOperator


@attr.s(auto_attribs=True)
class NoteBook(File):
    """Jupyter notebook"""

    type_hint: Optional[str] = "jupyter_notebook"
    parameters: Optional[Dict] = {}

    meta_schema: str = __name__ + '.NoteBook'


class PapermillOperator(BaseOperator):
    """
    Executes a jupyter notebook through papermill that is annotated with parameters

    :param input_nb: input notebook (can also be a NoteBook or a File inlet)
    :type input_nb: str
    :param output_nb: output notebook (can also be a NoteBook or File outlet)
    :type output_nb: str
    :param parameters: the notebook parameters to set
    :type parameters: dict
    """

    supports_lineage = True

    template_fields = ('input_nb', 'output_nb', 'parameters')

    def __init__(
        self,
        *,
        input_nb: Optional[str] = None,
        output_nb: Optional[str] = None,
        parameters: Optional[Dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.input_nb = input_nb
        self.output_nb = output_nb
        self.parameters = parameters
        if input_nb:
            self.inlets.append(NoteBook(url=input_nb, parameters=self.parameters))
        if output_nb:
            self.outlets.append(NoteBook(url=output_nb))

    def execute(self, context):
        if not self.inlets or not self.outlets:
            raise ValueError("Input notebook or output notebook is not specified")

        for i, item in enumerate(self.inlets):
            pm.execute_notebook(
                item.url,
                self.outlets[i].url,
                parameters=item.parameters,
                progress_bar=False,
                report_mode=True,
            )
