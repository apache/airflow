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

from typing import Any, Dict, Callable

from inspect import signature
from airflow.models.dag import DAG

import functools


class DagParam:
    """
    Class that represents a DAG run parameter.

    It can be used to parametrized your dags.

    **Example**:

        with DAG(...) as dag:
          EmailOperator(subject=dag.param('subject', 'Hi from Airflow!'))

    This object can be used in legacy Operators via Jinja.

    :param current_dag: Dag that will be used to pull the parameter from.
    :type current_dag: airflow.models.dag.DAG
    :param name: key value which is used to set the parameter
    :type name: str
    :param default: Default value used if no parameter was set.
    :type default: Any
    """

    def __init__(self, current_dag: DAG, name: str, default: Any):
        current_dag.params[name] = default
        self._name = name
        self._default = default

    def resolve(self, context: Dict) -> Any:
        """
        Pull DagParam value from DagRun context. This method is run during ``op.execute()``
        in respectable context.
        """
        return context.get('params', {}).get(self._name, self._default)


def dag(*dag_args, **dag_kwargs):
    """
    Python dag decorator. Wraps a function into an Airflow DAG.
    Accepts kwargs for operator kwarg. Can be used to parametrize DAGs.

    :param dag_args: Arguments for DAG object
    :type dag_args: list
    :param dag_kwargs: Kwargs for DAG object.
    :type dag_kwargs: dict
    """
    def wrapper(f: Callable):
        dag_sig = signature(DAG.__init__).bind(dag_id=f.__name__)
        dag_sig = dag_sig.bind(*dag_args, **dag_kwargs)
        @functools.wraps(f)
        def factory(*args, **kwargs):
            sig = signature(f).bind(*args, **kwargs).apply_defaults()
            with DAG(*dag_sig.args, **dag_sig.kwargs) as dag:
                f_kwargs = {}
                for name, value in sig.arguments.items():
                    f_kwargs[name] = dag.param(name, value)
                f(**f_kwargs)
            return dag
        return factory
    return wrapper
