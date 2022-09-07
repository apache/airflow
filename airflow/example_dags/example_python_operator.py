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
Example DAG demonstrating the usage of the TaskFlow API to execute Python functions natively and within a
virtual environment.
"""
import logging
import os
import shutil
import sys
import tempfile
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task

log = logging.getLogger(__name__)

PYTHON = sys.executable

BASE_DIR = tempfile.gettempdir()

with DAG(
    dag_id='example_python_operator',
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_python]
    @task(task_id="print_the_context")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    run_this = print_context()
    # [END howto_operator_python]

    # [START howto_operator_python_kwargs]
    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    for i in range(5):

        @task(task_id=f'sleep_for_{i}')
        def my_sleeping_function(random_base):
            """This is a function that will run within the DAG execution"""
            time.sleep(random_base)

        sleeping_task = my_sleeping_function(random_base=float(i) / 10)

        run_this >> sleeping_task
    # [END howto_operator_python_kwargs]

    if not shutil.which("virtualenv"):
        log.warning("The virtalenv_python example task requires virtualenv, please install it.")
    else:
        # [START howto_operator_python_venv]
        @task.virtualenv(
            task_id="virtualenv_python", requirements=["colorama==0.4.0"], system_site_packages=False
        )
        def callable_virtualenv():
            """
            Example function that will be performed in a virtual environment.

            Importing at the module level ensures that it will not attempt to import the
            library before it is installed.
            """
            from time import sleep

            from colorama import Back, Fore, Style

            print(Fore.RED + 'some red text')
            print(Back.GREEN + 'and with a green background')
            print(Style.DIM + 'and in dim text')
            print(Style.RESET_ALL)
            for _ in range(4):
                print(Style.DIM + 'Please wait...', flush=True)
                sleep(1)
            print('Finished')

        virtualenv_task = callable_virtualenv()
        # [END howto_operator_python_venv]

        sleeping_task >> virtualenv_task

        # [START howto_operator_external_python]
        @task.external_python(task_id="external_python", python=os.fspath(sys.executable))
        def callable_external_python():
            """
            Example function that will be performed in a virtual environment.

            Importing at the module level ensures that it will not attempt to import the
            library before it is installed.
            """
            import sys
            from time import sleep

            print(f"Running task via {sys.executable}")
            print("Sleeping")
            for _ in range(4):
                print('Please wait...', flush=True)
                sleep(1)
            print('Finished')

        external_python_task = callable_external_python()
        # [END howto_operator_external_python]

        run_this >> external_python_task
