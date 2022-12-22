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

Cluster Policies
================


.. autoapimodule:: airflow.policies
  :no-members:
  :members: task_policy, dag_policy, task_instance_mutation_hook, pod_mutation_hook
  :member-order: bysource


Examples
--------

DAG policies
~~~~~~~~~~~~

This policy checks if each DAG has at least one tag defined:

.. literalinclude:: /../../tests/cluster_policies/__init__.py
      :language: python
      :start-after: [START example_dag_cluster_policy]
      :end-before: [END example_dag_cluster_policy]

.. note::

    To avoid import cycles, if you use ``DAG`` in type annotations in your cluster policy, be sure to import from ``airflow.models`` and not from ``airflow``.

.. note::

    DAG policies are applied after the DAG has been completely loaded, so overriding the ``default_args`` parameter has no effect. If you want to override the default operator settings, use task policies instead.

Task policies
~~~~~~~~~~~~~

Here's an example of enforcing a maximum timeout policy on every task:

.. literalinclude:: /../../tests/cluster_policies/__init__.py
        :language: python
        :start-after: [START example_task_cluster_policy]
        :end-before: [END example_task_cluster_policy]

You could also implement to protect against common errors, rather than as technical security controls. For example, don't run tasks without airflow owners:

.. literalinclude:: /../../tests/cluster_policies/__init__.py
        :language: python
        :start-after: [START example_cluster_policy_rule]
        :end-before: [END example_cluster_policy_rule]

If you have multiple checks to apply, it is best practice to curate these rules in a separate python module and have a single policy / task mutation hook that performs multiple of these custom checks and aggregates the various error messages so that a single ``AirflowClusterPolicyViolation`` can be reported in the UI (and import errors table in the database).

For example, your ``airflow_local_settings.py`` might follow this pattern:

.. literalinclude:: /../../tests/cluster_policies/__init__.py
        :language: python
        :start-after: [START example_list_of_cluster_policy_rules]
        :end-before: [END example_list_of_cluster_policy_rules]


Task instance mutation
~~~~~~~~~~~~~~~~~~~~~~

Here's an example of re-routing tasks that are on their second (or greater) retry to a different queue:

.. literalinclude:: /../../tests/cluster_policies/__init__.py
        :language: python
        :start-after: [START example_task_mutation_hook]
        :end-before: [END example_task_mutation_hook]

Note that since priority weight is determined dynamically using weight rules, you cannot alter the ``priority_weight`` of a task instance within the mutation hook.
