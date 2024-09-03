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
"""Example DAG demonstrating the usage of the Classic branching Python operators.

It is showcasing the basic BranchPythonOperator and its sisters BranchExternalPythonOperator
and BranchPythonVirtualenvOperator."""

from __future__ import annotations

import random
import sys
import tempfile
from pathlib import Path

import pendulum

from airflow.operators.python import is_venv_installed

if is_venv_installed():
    from airflow.models.dag import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import (
        BranchExternalPythonOperator,
        BranchPythonOperator,
        BranchPythonVirtualenvOperator,
        ExternalPythonOperator,
        PythonOperator,
        PythonVirtualenvOperator,
    )
    from airflow.utils.edgemodifier import Label
    from airflow.utils.trigger_rule import TriggerRule

    PATH_TO_PYTHON_BINARY = sys.executable

    with DAG(
        dag_id="example_branch_operator",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule="@daily",
        tags=["example", "example2"],
        orientation="TB",
    ) as dag:
        run_this_first = EmptyOperator(
            task_id="run_this_first",
        )

        options = ["a", "b", "c", "d"]

        # Example branching on standard Python tasks

        # [START howto_operator_branch_python]
        branching = BranchPythonOperator(
            task_id="branching",
            python_callable=lambda: f"branch_{random.choice(options)}",
        )
        # [END howto_operator_branch_python]
        run_this_first >> branching

        join = EmptyOperator(
            task_id="join",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        for option in options:
            t = PythonOperator(
                task_id=f"branch_{option}",
                python_callable=lambda: print("Hello World"),
            )

            empty_follow = EmptyOperator(
                task_id="follow_" + option,
            )

            # Label is optional here, but it can help identify more complex branches
            branching >> Label(option) >> t >> empty_follow >> join

        # Example the same with external Python calls

        # [START howto_operator_branch_ext_py]
        def branch_with_external_python(choices):
            import random

            return f"ext_py_{random.choice(choices)}"

        branching_ext_py = BranchExternalPythonOperator(
            task_id="branching_ext_python",
            python=PATH_TO_PYTHON_BINARY,
            python_callable=branch_with_external_python,
            op_args=[options],
        )
        # [END howto_operator_branch_ext_py]
        join >> branching_ext_py

        join_ext_py = EmptyOperator(
            task_id="join_ext_python",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        def hello_world_with_external_python():
            print("Hello World from external Python")

        for option in options:
            t = ExternalPythonOperator(
                task_id=f"ext_py_{option}",
                python=PATH_TO_PYTHON_BINARY,
                python_callable=hello_world_with_external_python,
            )

            # Label is optional here, but it can help identify more complex branches
            branching_ext_py >> Label(option) >> t >> join_ext_py

        # Example the same with Python virtual environments

        # [START howto_operator_branch_virtualenv]
        # Note: Passing a caching dir allows to keep the virtual environment over multiple runs
        #       Run the example a second time and see that it re-uses it and is faster.
        VENV_CACHE_PATH = Path(tempfile.gettempdir())

        def branch_with_venv(choices):
            import random

            import numpy as np

            print(f"Some numpy stuff: {np.arange(6)}")
            return f"venv_{random.choice(choices)}"

        branching_venv = BranchPythonVirtualenvOperator(
            task_id="branching_venv",
            requirements=["numpy~=1.26.0"],
            venv_cache_path=VENV_CACHE_PATH,
            python_callable=branch_with_venv,
            op_args=[options],
        )
        # [END howto_operator_branch_virtualenv]
        join_ext_py >> branching_venv

        join_venv = EmptyOperator(
            task_id="join_venv",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        def hello_world_with_venv():
            import numpy as np

            print(f"Hello World with some numpy stuff: {np.arange(6)}")

        for option in options:
            t = PythonVirtualenvOperator(
                task_id=f"venv_{option}",
                requirements=["numpy~=1.26.0"],
                venv_cache_path=VENV_CACHE_PATH,
                python_callable=hello_world_with_venv,
            )

            # Label is optional here, but it can help identify more complex branches
            branching_venv >> Label(option) >> t >> join_venv
