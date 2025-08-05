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

.. versionadded:: 3.1

Human-in-the-Loop (HITL) functionality allows you to incorporate human decision-making directly into your workflows.
This powerful feature enables workflows to pause and wait for human input, making it perfect for approval processes, manual quality checks, and scenarios where human judgment is essential.

In this tutorial, we will explore how to use the HITL operators in workflows.

An HITL Example Dag
-------------------

Here is what HITL looks like in a Dag. We'll break it down and dive into it.

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :start-after: [START hitl_tutorial]
   :end-before: [END hitl_tutorial]


Input Provision
---------------

Users can provide input using params that is used for subsequent tasks.
This is useful for workflows involving human guidance within large language model (LLM) workflows.

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :dedent: 4
   :start-after: [START howto_hitl_entry_operator]
   :end-before: [END howto_hitl_entry_operator]


Option Selection
----------------

Input can be provided in the form of options.
Users can select one of the available options, which can be used to direct the workflow.

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :dedent: 4
   :start-after: [START howto_hitl_operator]
   :end-before: [END howto_hitl_operator]

Approval or Rejection
---------------------

A specialized form of option selection, which has only 'Approval' and 'Rejection' as options.

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :dedent: 4
   :start-after: [START howto_hitl_approval_operator]
   :end-before: [END howto_hitl_approval_operator]

As you can see in the body of this code snippet, you can use XComs to get information provided by the user.

Branch Selection
----------------

Users can choose which branches to follow within the Dag.
This is commonly applied in scenarios such as content moderation, where human judgment is sometimes required.

This is like option selection, but the option needs to be a task.
And remember to specify their relationship in the workflow.

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :dedent: 4
   :start-after: [START howto_hitl_branch_operator]
   :end-before: [END howto_hitl_branch_operator]

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :dedent: 4
   :start-after: [START howto_hitl_workflow]
   :end-before: [END howto_hitl_workflow]

Benefits and Common Use Cases
-----------------------------

HITL functionality is valuable in large language model (LLM) workflows, where human-provided guidance can be essential for achieving better results.
It is also highly beneficial in enterprise data pipelines, where human validation can complement and enhance automated processes.
