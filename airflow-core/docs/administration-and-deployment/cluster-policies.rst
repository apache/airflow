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

Cluster policies let you inspect or mutate Dags and tasks at the cluster level. They also let you apply
cluster-wide settings to Dags based on the ``dag_id`` or other properties.

Common use-cases include:

* Checking that Dags / tasks meet a certain standard
* Setting default arguments on Dags / tasks
* Performing custom routing logic

There are three main types of cluster policy:

* ``dag_policy``: Takes a :class:`~airflow.models.dag.DAG` parameter called ``dag``. Runs when the Dag is
  loaded from :class:`~airflow.models.dagbag.DagBag`.
* ``task_policy``: Takes a :class:`~airflow.models.baseoperator.BaseOperator` parameter called ``task``. Runs
  when the task is parsed from DagBag at load time, so the entire task definition can be modified. It is not
  tied to a specific DagRun — any changes apply to all future task instances of that task.
* ``task_instance_mutation_hook``: Takes a :class:`~airflow.models.taskinstance.TaskInstance` parameter called
  ``task_instance``. Unlike the task-level policies, this hook operates on a single task instance within a
  specific DagRun. It runs on the worker just before the task instance is executed, so changes apply only to
  that particular run.

Dag and task cluster policies can raise :class:`~airflow.exceptions.AirflowClusterPolicyViolation`
to indicate that the Dag or task is not compliant and should not be loaded.

They can also raise :class:`~airflow.exceptions.AirflowClusterPolicySkipDag` to intentionally skip a Dag.
Unlike :class:`~airflow.exceptions.AirflowClusterPolicyViolation`, this exception is not shown in the
Airflow UI and is not recorded in the ``import_error`` table in the metadata database.

Any extra attributes set by a cluster policy take priority over those defined in your Dag file; for example,
if you set an ``sla`` on your Task in the Dag file, and then your cluster policy also sets an ``sla``, the
cluster policy's value will take precedence.

.. _administration-and-deployment:cluster-policies-define:

How to define a policy function
-------------------------------

There are two ways to configure cluster policies:

1. Create an ``airflow_local_settings.py`` file somewhere on the Python path (the ``config/`` folder
   under ``$AIRFLOW_HOME`` is the recommended location), then add callables matching one or more of the
   cluster policy names above (e.g. ``dag_policy``).

   See :ref:`Configuring local settings <set-config:configuring-local-settings>` for details.

2. Use a
   `setuptools entrypoint <https://packaging.python.org/guides/creating-and-discovering-plugins/#using-package-metadata>`_
   in a custom module using the `Pluggy <https://pluggy.readthedocs.io/en/stable/>`_ interface.

   .. versionadded:: 2.6

   This method is more advanced and assumes familiarity with Python packaging.

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

   The entrypoint group must be ``airflow.policy``. Each entry name must be unique — pluggy silently
   ignores duplicates. The value must be the module (or class) decorated with the ``@hookimpl`` marker.

   After installing the distribution into your Airflow environment, the policy functions will be called by
   the relevant Airflow components. If you register multiple plugins, the call order between them is
   undefined, so do not rely on a specific ordering.


Regardless of which method you use, argument names in your policy functions must exactly match the
signatures documented below.

Available Policy Functions
--------------------------

.. autoapimodule:: airflow.policies
  :no-members:
  :members: task_policy, dag_policy, task_instance_mutation_hook, pod_mutation_hook, get_airflow_context_vars
  :member-order: bysource


Examples
--------

Dag policies
~~~~~~~~~~~~

This policy checks if each Dag has at least one tag defined:

.. literalinclude:: /../tests/unit/cluster_policies/__init__.py
      :language: python
      :start-after: [START example_dag_cluster_policy]
      :end-before: [END example_dag_cluster_policy]

.. note::

    To avoid import cycles, if you use ``DAG`` in type annotations in your cluster policy, be sure to import from ``airflow.models`` and not from ``airflow``.

.. note::

    Dag policies are applied after the Dag has been completely loaded, so overriding the ``default_args`` parameter has no effect. If you want to override the default operator settings, use task policies instead.

Task policies
~~~~~~~~~~~~~

Here's an example of enforcing a maximum timeout policy on every task:

.. literalinclude:: /../tests/unit/cluster_policies/__init__.py
        :language: python
        :start-after: [START example_task_cluster_policy]
        :end-before: [END example_task_cluster_policy]

You can also use a policy to guard against common authoring errors rather than enforce security controls. For example, requiring all tasks to have an Airflow owner:

.. literalinclude:: /../tests/unit/cluster_policies/__init__.py
        :language: python
        :start-after: [START example_cluster_policy_rule]
        :end-before: [END example_cluster_policy_rule]

If you have multiple checks to apply, define each rule in a separate function and call them all from a
single policy function. This lets you aggregate error messages and raise a single
``AirflowClusterPolicyViolation`` — which is what gets shown in the UI and recorded in the import errors table.

For example, your ``airflow_local_settings.py`` might follow this pattern:

.. literalinclude:: /../tests/unit/cluster_policies/__init__.py
        :language: python
        :start-after: [START example_list_of_cluster_policy_rules]
        :end-before: [END example_list_of_cluster_policy_rules]

See :ref:`Configuring local settings <set-config:configuring-local-settings>` for details on how to
configure local settings.


Task instance mutation
~~~~~~~~~~~~~~~~~~~~~~

Here's an example of re-routing tasks that are on their second (or greater) retry to a different queue:

.. literalinclude:: /../tests/unit/cluster_policies/__init__.py
        :language: python
        :start-after: [START example_task_mutation_hook]
        :end-before: [END example_task_mutation_hook]

Note that ``priority_weight`` cannot be altered in the mutation hook — it is determined dynamically by weight rules.


Metadata Engine Hooks
---------------------

In addition to cluster policies, ``airflow_local_settings.py`` can override how Airflow creates its metadata
database engines. This is useful when you need per-connection logic that cannot be expressed through static
configuration — for example, injecting short-lived JWT tokens or IAM credentials via a SQLAlchemy
``do_connect`` event handler.

Two functions can be overridden:

* ``create_metadata_engine(sql_alchemy_conn, *, engine_args, connect_args) -> Engine`` — called by
  ``configure_orm()`` to create the synchronous metadata engine.
* ``create_async_metadata_engine(sql_alchemy_conn_async, *, connect_args) -> AsyncEngine`` — called by
  ``_configure_async_session()`` to create the asynchronous metadata engine.

The default implementations call ``sqlalchemy.create_engine`` / ``sqlalchemy.ext.asyncio.create_async_engine``
with the same arguments Airflow has always used, so there is **no behavioral change** unless you provide an
override.

Example: registering a ``do_connect`` handler that refreshes a JWT token before every new physical connection:

.. code-block:: python

    # airflow_local_settings.py
    from sqlalchemy import create_engine, event


    def _refresh_jwt(dbapi_connection, connection_record):
        """Called before every physical connection (including after pool recycle)."""
        token = my_token_provider.get_token()
        dbapi_connection.execute(f"SET SESSION AUTHORIZATION '{token}'")


    def create_metadata_engine(sql_alchemy_conn, *, engine_args, connect_args):
        engine = create_engine(
            sql_alchemy_conn,
            connect_args=connect_args,
            **engine_args,
            future=True,
        )
        event.listen(engine, "do_connect", _refresh_jwt)
        return engine
