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
"""DAG Cycle tester"""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models.dag import DAG


def test_cycle(dag: 'DAG') -> None:
    """
    A wrapper function of `check_cycle` for backward compatibility purpose.
    New code should use `check_cycle` instead since this function name `test_cycle` starts with 'test_' and
    will be considered as a unit test by pytest, resulting in failure.
    """
    from warnings import warn

    warn(
        "Deprecated, please use `check_cycle` at the same module instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return check_cycle(dag)


def check_cycle(dag: 'DAG') -> None:
    """Check to see if there are any cycles in the DAG.

    :raises AirflowDagCycleException: If cycle is found in the DAG.
    """
    dag.topological_sort(include_subdag_tasks=False)
