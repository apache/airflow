 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

 .. code-block:: python

    from airflow.models.taskinstance import TaskInstance
    from airflow.models.dagrun import DagRun


    @task
    def print_ti_info(task_instance: TaskInstance, dag_run: DagRun):
        print(f"Run ID: {task_instance.run_id}")  # Run ID: scheduled__2023-08-09T00:00:00+00:00
        print(f"Duration: {task_instance.duration}")  # Duration: 0.972019
        print(f"Dag Run queued at: {dag_run.queued_at}")  # 2023-08-10 00:00:01+02:20
