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
Example DAG demonstrating the usage of the TaskFlow API to execute Python functions
natively and within a virtual environment.
"""

from __future__ import annotations

import logging
import sys
import time
from pprint import pprint

import pendulum

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    doc_md="""
    ### TaskFlow API: Python Task Execution and Isolation

    The TaskFlow API (`@task` decorator) provides a Pythonic alternative to PythonOperator, with key advantages: automatic dependency inference from return values, simplified XCom handling, and improved code readability. Functions decorated with `@task` are tasks in your DAG graph, not Python functions called during parsing—this distinction enables proper task isolation, testing, and dynamic task generation.

    **When to use TaskFlow over PythonOperator:**
    - Complex data pipelines where tasks pass structured data (dicts, objects); TaskFlow's implicit XCom handling reduces boilerplate
    - Workflows requiring high code readability and maintainability; Python functions are easier to test and understand than operator instantiation
    - Dynamic task generation and conditional task creation; decorator syntax integrates seamlessly with Python control flow

    **Dependency isolation with virtualenv and external Python:**
    - `@task.virtualenv`: Creates isolated Python environments per task, essential when tasks require conflicting libraries (e.g., TensorFlow 1.x vs. 2.x in the same DAG) or strict reproducibility requirements
    - `@task.external_python`: Delegates execution to a pre-installed Python interpreter, reducing overhead when isolation is needed but virtualenv creation is too expensive
    - Both patterns prevent DAG-level package conflicts and enable using multiple Python versions in a single workflow

    **Important behavior:**
    - TaskFlow functions execute at task runtime in the chosen environment, not during DAG parsing; heavy imports inside functions avoid DAG initialization overhead
    - Return values are automatically serialized to XCom; XCom limits and serialization backends apply
    - Virtualenv and external_python tasks consume additional executor resources and network bandwidth for environment setup

    📖 [TaskFlow API Documentation](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
    """
)
def example_python_decorator():
    # [START howto_operator_python]
    @task(task_id="print_the_context")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return "Whatever you return gets printed in the logs"

    run_this = print_context()
    # [END howto_operator_python]

    # [START howto_operator_python_render_sql]
    @task(task_id="log_sql_query", templates_dict={"query": "sql/sample.sql"}, templates_exts=[".sql"])
    def log_sql(**kwargs):
        log.info("Python task decorator query: %s", str(kwargs["templates_dict"]["query"]))

    log_the_sql = log_sql()
    # [END howto_operator_python_render_sql]

    # [START howto_operator_python_kwargs]
    @task
    def my_sleeping_function(random_base):
        """This is a function that will run within the DAG execution."""
        time.sleep(random_base)

    for i in range(5):
        sleeping_task = my_sleeping_function.override(task_id=f"sleep_for_{i}")(random_base=i / 10)
        run_this >> log_the_sql >> sleeping_task
    # [END howto_operator_python_kwargs]

    # [START howto_operator_python_venv]
    @task.virtualenv(
        task_id="virtualenv_python",
        requirements=["colorama==0.4.0"],
        system_site_packages=False,
    )
    def callable_virtualenv():
        """
        Example function that will be performed in a virtual environment.

        Importing at the module level ensures that it will not attempt
        to import the library before it is installed.
        """
        from time import sleep

        from colorama import Back, Fore, Style

        print(Fore.RED + "some red text")
        print(Back.GREEN + "and with a green background")
        print(Style.DIM + "and in dim text")
        print(Style.RESET_ALL)
        for _ in range(4):
            print(Style.DIM + "Please wait...", flush=True)
            sleep(1)
        print("Finished")

    virtualenv_task = callable_virtualenv()
    # [END howto_operator_python_venv]

    sleeping_task >> virtualenv_task

    # [START howto_operator_external_python]
    @task.external_python(task_id="external_python", python=PATH_TO_PYTHON_BINARY)
    def callable_external_python():
        """
        Example function that will be performed using an external
        Python interpreter.
        """
        import sys
        from time import sleep

        print(f"Running task via {sys.executable}")
        print("Sleeping")
        for _ in range(4):
            print("Please wait...", flush=True)
            sleep(1)
        print("Finished")

    external_python_task = callable_external_python()
    # [END howto_operator_external_python]

    run_this >> external_python_task >> virtualenv_task


example_dag = example_python_decorator()
