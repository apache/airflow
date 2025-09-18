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

In this tutorial, we will explore how to use the HITL operators in workflows and demonstrate how it would look like in Airflow UI.

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

|

You can click the task and find the Required Actions tab in the details panel.

.. image:: /img/ui-light/hitl_wait_for_input.png
  :alt: Demo HITL task instance waiting for input

|

Option Selection
----------------

Input can be provided in the form of options.
Users can select one of the available options, which can be used to direct the workflow.

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :dedent: 4
   :start-after: [START howto_hitl_operator]
   :end-before: [END howto_hitl_operator]

|

.. image:: /img/ui-light/hitl_wait_for_option.png
  :alt: Demo HITL task instance waiting for an option

|

Multiple options are also allowed.

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :dedent: 4
   :start-after: [START howto_hitl_operator_multiple]
   :end-before: [END howto_hitl_operator_multiple]

|

.. image:: /img/ui-light/hitl_wait_for_multiple_options.png
  :alt: Demo HITL task instance waiting for multiple options

|

Approval or Rejection
---------------------

A specialized form of option selection, which has only 'Approval' and 'Rejection' as options.
You can also set the ``assigned_users`` to restrict the users allowed to respond for a HITL operator.
It should be a list of user ids and user names (both needed) (e.g., ``[{"id": "1", "name": "user1"}, {"id": "2", "name": "user2"}]``.
ONLY the users within this list will be allowed to respond.

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :dedent: 4
   :start-after: [START howto_hitl_approval_operator]
   :end-before: [END howto_hitl_approval_operator]

|

As you can see in the body of this code snippet, you can use XComs to get information provided by the user.

.. image:: /img/ui-light/hitl_approve_reject.png
  :alt: Demo HITL task instance waiting for approval or rejection

|

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

|

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :dedent: 4
   :start-after: [START howto_hitl_workflow]
   :end-before: [END howto_hitl_workflow]

|

.. image:: /img/ui-light/hitl_branch_selection.png
  :alt: Demo HITL task instance waiting for branch selection

|

After the branch is chosen, the workflow will proceed along the selected path.

.. image:: /img/ui-light/hitl_branch_selected.png
  :alt: Demo HITL task instance after branch selection

Notifiers
---------

A notifier is a callback mechanism for handling HITL events, such as when a task is waiting for human input, succeeds, or fails.
The example uses the ``LocalLogNotifier``, which logs messages for demonstration purposes.

The method ``HITLOperator.generate_link_to_ui_from_context`` can be used to generate a direct link to the UI page where the user should respond. It accepts four arguments:

- ``context`` – automatically passed to ``notify`` by the notifier
- ``base_url`` – (optional) the base URL of the Airflow UI; if not provided, ``api.base_url`` in the configuration will be used
- ``options`` – (optional) pre-selected options for the UI page
- ``params_inputs`` – (optional) pre-loaded inputs for the UI page

This makes it easy to include actionable links in notifications or logs.
You can also implement your own notifier to provide different functionalities.
For more details, please refer to `Creating a notifier <https://airflow.apache.org/docs/apache-airflow/stable/howto/notifications.html>`_ and `Notifications <https://airflow.apache.org/docs/apache-airflow-providers/core-extensions/notifications.html>`_.

In the example Dag, the notifier is defined as follows:

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :start-after: [START hitl_notifier]
   :end-before: [END hitl_notifier]

|

You can pass a list of notifiers to HITL operators using the ``notifiers`` argument as follows.
When the operator creates an HITL request that is waiting for a human response, the ``notify`` method will be called with a single argument, ``context``.

.. exampleinclude:: /../../providers/standard/src/airflow/providers/standard/example_dags/example_hitl_operator.py
   :language: python
   :dedent: 4
   :start-after: [START howto_hitl_entry_operator]
   :end-before: [END howto_hitl_entry_operator]


Benefits and Common Use Cases
-----------------------------

HITL functionality is valuable in large language model (LLM) workflows, where human-provided guidance can be essential for achieving better results.
It is also highly beneficial in enterprise data pipelines, where human validation can complement and enhance automated processes.
