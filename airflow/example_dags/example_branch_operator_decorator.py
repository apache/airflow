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
"""Example DAG demonstrating the usage of the branching TaskFlow API decorators.

It shows how to use standard Python ``@task.branch`` as well as the external Python
version ``@task.branch_external_python`` which calls an external Python interpreter and
the ``@task.branch_virtualenv`` which builds a temporary Python virtual environment.
"""

from __future__ import annotations

import random
import sys
import tempfile

import pendulum

from airflow.providers.standard.core.operators.python import is_venv_installed

if is_venv_installed():
    from airflow.decorators import task
    from airflow.models.dag import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.utils.edgemodifier import Label
    from airflow.utils.trigger_rule import TriggerRule

    PATH_TO_PYTHON_BINARY = sys.executable

    with DAG(
        dag_id="example_branch_python_operator_decorator",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule="@daily",
        tags=["example", "example2"],
        orientation="TB",
    ) as dag:
        run_this_first = EmptyOperator(task_id="run_this_first")

        options = ["a", "b", "c", "d"]

        # Example branching on standard Python tasks

        # [START howto_operator_branch_python]
        @task.branch()
        def branching(choices: list[str]) -> str:
            return f"branch_{random.choice(choices)}"

        # [END howto_operator_branch_python]

        random_choice_instance = branching(choices=options)

        run_this_first >> random_choice_instance

        join = EmptyOperator(task_id="join", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

        for option in options:

            @task(task_id=f"branch_{option}")
            def some_task():
                print("doing something in Python")

            t = some_task()
            empty = EmptyOperator(task_id=f"follow_{option}")

            # Label is optional here, but it can help identify more complex branches
            random_choice_instance >> Label(option) >> t >> empty >> join

        # Example the same with external Python calls

        # [START howto_operator_branch_ext_py]
        @task.branch_external_python(python=PATH_TO_PYTHON_BINARY)
        def branching_ext_python(choices) -> str:
            import random

            return f"ext_py_{random.choice(choices)}"

        # [END howto_operator_branch_ext_py]

        random_choice_ext_py = branching_ext_python(choices=options)

        join >> random_choice_ext_py

        join_ext_py = EmptyOperator(
            task_id="join_ext_py", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )

        for option in options:

            @task.external_python(task_id=f"ext_py_{option}", python=PATH_TO_PYTHON_BINARY)
            def some_ext_py_task():
                print("doing something in external Python")

            t = some_ext_py_task()

            # Label is optional here, but it can help identify more complex branches
            random_choice_ext_py >> Label(option) >> t >> join_ext_py

        # Example the same with Python virtual environments

        # [START howto_operator_branch_virtualenv]
        # Note: Passing a caching dir allows to keep the virtual environment over multiple runs
        #       Run the example a second time and see that it re-uses it and is faster.
        VENV_CACHE_PATH = tempfile.gettempdir()

        @task.branch_virtualenv(requirements=["numpy~=1.24.4"], venv_cache_path=VENV_CACHE_PATH)
        def branching_virtualenv(choices) -> str:
            import random

            import numpy as np

            print(f"Some numpy stuff: {np.arange(6)}")
            return f"venv_{random.choice(choices)}"

        # [END howto_operator_branch_virtualenv]

        random_choice_venv = branching_virtualenv(choices=options)

        join_ext_py >> random_choice_venv

        join_venv = EmptyOperator(task_id="join_venv", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

        for option in options:

            @task.virtualenv(
                task_id=f"venv_{option}", requirements=["numpy~=1.24.4"], venv_cache_path=VENV_CACHE_PATH
            )
            def some_venv_task():
                import numpy as np

                print(f"Some numpy stuff: {np.arange(6)}")

            t = some_venv_task()

            # Label is optional here, but it can help identify more complex branches
            random_choice_venv >> Label(option) >> t >> join_venv
