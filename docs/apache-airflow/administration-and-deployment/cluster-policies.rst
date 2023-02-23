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

If you want to check or mutate DAGs or Tasks on a cluster-wide level, then a Cluster Policy will let you do
that. They have three main purposes:

* Checking that DAGs/Tasks meet a certain standard
* Setting default arguments on DAGs/Tasks
* Performing custom routing logic

There are three main types of cluster policy:

* ``dag_policy``: Takes a :class:`~airflow.models.dag.DAG` parameter called ``dag``. Runs at load time of the
  DAG from DagBag :class:`~airflow.models.dagbag.DagBag`.
* ``task_policy``: Takes a :class:`~airflow.models.baseoperator.BaseOperator` parameter called ``task``. The
  policy gets executed when the task is created during parsing of the task from DagBag at load time. This
  means that the whole task definition can be altered in the task policy. It does not relate to a specific
  task running in a DagRun. The ``task_policy`` defined is applied to all the task instances that will be
  executed in the future.
* ``task_instance_mutation_hook``: Takes a :class:`~airflow.models.taskinstance.TaskInstance` parameter called
  ``task_instance``. The ``task_instance_mutation_hook`` applies not to a task but to the instance of a task that
  relates to a particular DagRun. It is executed in a "worker", not in the dag file processor, just before the
  task instance is executed. The policy is only applied to the currently executed run (i.e. instance) of that
  task.

The DAG and Task cluster policies can raise the  :class:`~airflow.exceptions.AirflowClusterPolicyViolation`
exception to indicate that the dag/task they were passed is not compliant and should not be loaded.

Any extra attributes set by a cluster policy take priority over those defined in your DAG file; for example,
if you set an ``sla`` on your Task in the DAG file, and then your cluster policy also sets an ``sla``, the
cluster policy's value will take precedence.


How do define a policy function
-------------------------------

There are two ways to configure cluster policies:

1. create an ``airflow_local_settings.py`` file somewhere in the python search path (the ``config/`` folder
   under your $AIRFLOW_HOME is a good "default" location) and then add callables to the file matching one or more
   of the cluster policy names above (e.g. ``dag_policy``).

2. By using a
   `setuptools entrypoint <https://packaging.python.org/guides/creating-and-discovering-plugins/#using-package-metadata>`_
   in a custom module using the `Pluggy <https://pluggy.readthedocs.io/en/stable/>`_ interface.

   .. versionadded:: 2.6

   .. note:: |experimental|

   This method is more advanced for for people who are already comfortable with python packaging.

   First create your policy function in a module:

   .. code-block:: python

    from airflow.policies import hookimpl


    @hookimpl
    def task_policy(task) -> None:
        # Mutate task in place
        # ...
        print(f"Hello from {__file__}")

   And then add the entrypoint to your project specification. For example, using ``pyproject.toml`` and ``setuptools``:

   .. code-block:: toml

    [build-system]
    requires = ["setuptools", "wheel"]
    build-backend = "setuptools.build_meta"

    [project]
    name = "my-airflow-plugin"
    version = "0.0.1"
    # ...

    dependencies = ["apache-airflow>=2.6"]
    [project.entry-points.'airflow.policy']
    _ = 'my_airflow_plugin.policies'

   The entrypoint group must be ``airflow.policy``, and the name is ignored. The value should be your module (or class) decorated with the ``@hookimpl`` marker

   One you have done that, and you have installed your distribution into your Airflow env the policy functions will get called by the various Airflow components. (The exact call order is undefined, so don't rely on any particular calling order if you have multiple plugins).


One important thing to note (for either means of defining policy functions) is that the argument names must
exactly match as documented below.

Available Policy Functions
--------------------------

.. autoapimodule:: airflow.policies
  :no-members:
  :members: task_policy, dag_policy, task_instance_mutation_hook, pod_mutation_hook, get_airflow_context_vars
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
