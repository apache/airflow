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

from typing import Any, Dict


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

    def __init__(self, current_dag, name: str, default: Any):
        current_dag.params[name] = default
        self._name = name
        self._default = default

    def resolve(self, context: Dict) -> Any:
        """
        Pull DagParam value from DagRun context. This method is run during ``op.execute()``
        in respectable context.
        """
        return context.get('params', {}).get(self._name, self._default)
