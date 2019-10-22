# -*- coding: utf-8 -*-
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
from typing import Dict

import papermill as pm

from airflow.lineage.datasets import DataSet
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class NoteBook(DataSet):
    type_name = "jupyter_notebook"
    attributes = ['location', 'parameters']


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
    @apply_defaults
    def __init__(self,
                 input_nb: str,
                 output_nb: str,
                 parameters: Dict,
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.inlets.append(NoteBook(qualified_name=input_nb,
                                    location=input_nb,
                                    parameters=parameters))
        self.outlets.append(NoteBook(qualified_name=output_nb,
                                     location=output_nb))

    def execute(self, context):
        for i in range(len(self.inlets)):
            pm.execute_notebook(self.inlets[i].location, self.outlets[i].location,
                                parameters=self.inlets[i].parameters,
                                progress_bar=False, report_mode=True)
