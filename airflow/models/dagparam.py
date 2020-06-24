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

from typing import Any, Dict, List, Union

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG  # pylint: disable=R0401
from airflow.models.xcom import XCOM_RETURN_KEY
from inspect import signature, Parameter
from airflow.exceptions import AirflowException
from airflow.models.dag import DAG

import functools

class DagParam:
    """
    
    """

    def __init__(self, dag: DAG, name: str, default: Any):
        dag.params[name] = default
        self._name = name
        self._default = default

    def resolve(self, context: Dict) -> Any:
        """
        Pull XCom value for the existing arg. This method is run during ``op.execute()``
        in respectable context.
        """
        return self.context.get('params', {}).get(self._name, self._default)


def dag(*dag_args, **dag_kwargs):
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