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

import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.sdk.definitions.dag import DAG

warnings.warn(
    "`airflow.utils.dag_cycle_tester` module is deprecated and and will be removed in a future release."
    "Please use `dag.check_cycle()` method instead.",
    DeprecationWarning,
    stacklevel=2,
)


def check_cycle(dag: DAG) -> None:
    """
    Check to see if there are any cycles in the DAG.

    .. deprecated:: 3.1.0
        This function is deprecated. Use `dag.check_cycle()` method instead.

    :param dag: The DAG to check for cycles
    :raises AirflowDagCycleException: If cycle is found in the DAG.
    """
    # No warning here since we already warned on import
    dag.check_cycle()


__all__ = ["check_cycle"]
