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

HITLOperator (Human-in-the-loop)
================================

Human-in-the-Loop (HITL) functionality allows you to incorporate human decision-making directly into your workflows.
This powerful feature enables workflows to pause and wait for human input, making it perfect for approval processes, manual quality checks, and scenarios where human judgment is essential.
If you are still unsure about its usage and benefits, the following examples will guide you through them.

Example flows include:

Branch selection
----------------

Users can choose which branch to follow within the DAG.
This is commonly applied in scenarios such as content moderation, where human judgment is sometimes required.

.. exampleinclude:: /../../providers/standard/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :start-after: [START howto_hitl_branch_operator]
   :end-before: [END howto_hitl_branch_operator]

Approval or Rejection
---------------------

A specialized form of branch selection.
Users can approve or reject, thereby directing the workflow along different branches.

.. exampleinclude:: /../../providers/standard/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :start-after: [START howto_hitl_approval_operator]
   :end-before: [END howto_hitl_approval_operator]

Input provision
---------------

Users can provide input that is used for subsequent tasks.
This is useful for workflows involving human guidance within large language model (LLM) workflosws.

.. exampleinclude:: /../../providers/standard/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :start-after: [START howto_hitl_entry_operator]
   :end-before: [END howto_hitl_entry_operator]
