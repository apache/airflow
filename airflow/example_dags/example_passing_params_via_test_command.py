# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG("example_passing_params_via_test_command",
          default_args={"owner": "airflow",
                        "start_date":datetime.now()},
          schedule_interval='*/1 * * * *',
          dagrun_timeout=timedelta(minutes=4)
          )


def my_py_command(ds, **kwargs):
    # Print out the "foo" param passed in via
    # `airflow test example_passing_params_via_test_command run_this <date>
    # -tp '{"foo":"bar"}'`
    if kwargs["test_mode"]:
        print(" 'foo' was passed in via test={} command : kwargs[params][foo] \
               = {}".format(kwargs["test_mode"], kwargs["params"]["foo"]))
    # Print out the value of "miff", passed in below via the Python Operator
    print(" 'miff' was passed in via task params = {}".format(kwargs["params"]["miff"]))
    return 1

my_templated_command = """
    echo " 'foo was passed in via Airflow CLI Test command with value {{ params.foo }} "
    echo " 'miff was passed in via BashOperator with value {{ params.miff }} "
"""

run_this = PythonOperator(
    task_id='run_this',
    provide_context=True,
    python_callable=my_py_command,
    params={"miff":"agg"},
    dag=dag)

also_run_this = BashOperator(
    task_id='also_run_this',
    bash_command=my_templated_command,
    params={"miff":"agg"},
    dag=dag)
also_run_this.set_upstream(run_this)
