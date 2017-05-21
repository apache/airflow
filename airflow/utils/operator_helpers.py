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


def context_to_airflow_vars(context):
    """
    Given a context, this function provides a dictionary of values that can be used to
    externally reconstruct relations between dags, dag_runs, tasks and task_instances.

    :param context: The context for the task_instance of interest
    :type context: dict
    """
    params = {}
    dag = context.get('dag')
    if dag and dag.dag_id:
        params['airflow.ctx.dag.dag_id'] = dag.dag_id

    dag_run = context.get('dag_run')
    if dag_run and dag_run.execution_date:
        params['airflow.ctx.dag_run.execution_date'] = dag_run.execution_date.isoformat()

    task = context.get('task')
    if task and task.task_id:
        params['airflow.ctx.task.task_id'] = task.task_id

    task_instance = context.get('task_instance')
    if task_instance and task_instance.execution_date:
        params['airflow.ctx.task_instance.execution_date'] = (
            task_instance.execution_date.isoformat()
        )

    return params
