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
from __future__ import annotations

# [START tutorial]
# [START import_module]
import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# [END import_module]


# [START instantiate_dag]
@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    params={"foobar": "param_from_dag", "other_param": "from_dag"},
)
def tutorial_taskflow_templates():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple data pipeline example which demonstrates the use of
    the templates in the TaskFlow API.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    # [END instantiate_dag]

    # [START template_test]
    @task(
        # Causes variables that end with `.sql` to be read and templates
        # within to be rendered.
        templates_exts=[".sql"],
    )
    def template_test(sql, test_var, data_interval_end):
        context = get_current_context()

        # Will print...
        # select * from test_data
        # where 1=1
        #     and run_id = 'scheduled__2024-10-09T00:00:00+00:00'
        #     and something_else = 'param_from_task'
        print(f"sql: {sql}")

        # Will print `scheduled__2024-10-09T00:00:00+00:00`
        print(f"test_var: {test_var}")

        # Will print `2024-10-10 00:00:00+00:00`.
        # Note how we didn't pass this value when calling the task. Instead
        # it was passed by the decorator from the context
        print(f"data_interval_end: {data_interval_end}")

        # Will print...
        # run_id: scheduled__2024-10-09T00:00:00+00:00; params.other_param: from_dag
        template_str = (
            "run_id: {{ run_id }}; params.other_param: {{ params.other_param }}"
        )
        rendered_template = context["task"].render_template(
            template_str,
            context,
        )
        print(f"rendered template: {rendered_template}")

        # Will print the full context dict
        print(f"context: {context}")

    # [END template_test]

    # [START main_flow]
    template_test.override(
        # Will be merged with the dict defined in the dag
        # and override existing parameters.
        #
        # Must be passed into the decorator's parameters
        # through `.override()` not into the actual task
        # function
        params={"foobar": "param_from_task"},
    )(
        sql="sql/test.sql",
        test_var="{{ run_id }}",
    )
    # [END main_flow]


# [START dag_invocation]
tutorial_taskflow_templates()
# [END dag_invocation]

# [END tutorial]
