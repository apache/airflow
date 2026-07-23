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

Organizing tasks with TaskGroups
---------------------------------

When a Dag has many related tasks, you can group them visually using
:class:`~airflow.sdk.TaskGroup`. This keeps the Graph view readable and
lets you organize tasks into logical sections, including nested groups.

.. exampleinclude:: /../src/airflow/example_dags/example_task_group.py
    :language: python
    :start-after: [START howto_task_group_section_1]
    :end-before: [END howto_task_group_section_1]

TaskGroups can also be nested inside one another:

.. exampleinclude:: /../src/airflow/example_dags/example_task_group.py
    :language: python
    :start-after: [START howto_task_group_section_2]
    :end-before: [END howto_task_group_section_2]
