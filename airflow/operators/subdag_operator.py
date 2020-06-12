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
"""
The module which provides a way to nest your DAGs and so your levels of complexity.
"""
from typing import Callable, Optional
from cached_property import cached_property

from airflow.models.dag import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class SubDagOperator(BaseOperator):
    """
    This creates a SubDag. A SubDag's tasks will be recursively unpacked and append
    to the root DAG during parsing.

    The factory function should satisfy the following signature.

    def dag_factory(dag_id, ...):
        dag = DAG(
            dag_id=dag_id,
            ...
        )

    The first positional argument must be a dag_id passing to the DAG constructor. Internally,
    it will be passed with the operator.task_id to create metadata to render grouping in the UI.

    :param subdag_factory: a DAG factory function that returns a dag when called
    :param subdag_args: a list of positional arguments that will get unpacked when
        calling the factory function
    :param subdag_kwargs: a dictionary of keyword arguments that will get unpacked
        in the factory function
    """

    ui_color = '#555'
    ui_fgcolor = '#fff'

    @apply_defaults
    def __init__(self,
                 subdag_factory: Callable[..., DAG],
                 subdag_args: Optional[list] = None,
                 subdag_kwargs: Optional[dict] = None,
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.subdag_args = subdag_args or []
        self.subdag_kwargs = subdag_kwargs or {}
        self.subdag_factory = subdag_factory

    @cached_property
    def subdag(self) -> DAG:
        """The SubDag carried by the operator"""
        self._subdag = self.subdag_factory(self.task_id, *self.subdag_args, **self.subdag_kwargs)
        return self._subdag
