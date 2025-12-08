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

Public Interface for Airflow 3.0+
=================================

.. warning::

   This documentation covers the Public Interface for Airflow 3.0+

   If you are using Airflow 2.x, please refer to the
   `Airflow 2.11 Public Interface Documentation <https://airflow.apache.org/docs/apache-airflow/2.11.0/public-airflow-interface.html>`_
   for the legacy interface.

The Public Interface of Apache Airflow is the collection of interfaces and behaviors in Apache Airflow
whose changes are governed by semantic versioning. A user interacts with Airflow's public interface
by creating and managing Dags, managing tasks and dependencies,
and extending Airflow capabilities by writing new executors, plugins, operators and providers. The
Public Interface can be useful for building custom tools and integrations with other systems,
and for automating certain aspects of the Airflow workflow.

The primary public interface for Dag authors and task execution is using task SDK
Airflow task SDK is the primary public interface for Dag authors and for task execution
:doc:`airflow.sdk namespace <core-concepts/taskflow>`. Direct access to the metadata database
from task code is no longer allowed. Instead, use the :doc:`Stable REST API <stable-rest-api-ref>`,
`Python Client <https://github.com/apache/airflow-client-python>`_, or Task Context methods.

For comprehensive Task SDK documentation, see the `Task SDK Reference <https://airflow.apache.org/docs/task-sdk/stable/>`_.

Using Airflow Public Interfaces
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

   As of **Airflow 3.0**, users should use the ``airflow.sdk`` namespace as the official **Public Interface**, as defined in `AIP-72 <https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-72+Task+Execution+Interface+aka+Task+SDK>`_.

   Direct interaction with internal modules or the metadata database is not possible.
   For stable, production-safe integration, it is recommended to use:

   - The official **REST API**
   - The **Python Client SDK** (`airflow-client-python`)
   - The new **Task SDK** (``airflow.sdk``)

   Related docs:
   - `Release Notes 3.0 <https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html>`_
   - `Task SDK Overview <https://airflow.apache.org/docs/apache-airflow/stable/concepts/taskflow.html>`_

The following are some examples of the public interface of Airflow:

* When you are writing your own operators or hooks. This is commonly done when no hook or operator exists for your use case, or when perhaps when one exists but you need to customize the behavior.
* When writing new :doc:`Plugins <administration-and-deployment/plugins>` that extend Airflow's functionality beyond
  Dag building blocks. Secrets, Timetables, Triggers, Listeners are all examples of such functionality. This
  is usually done by users who manage Airflow instances.
* Bundling custom Operators, Hooks, Plugins and releasing them together via
  :doc:`providers <apache-airflow-providers:index>` - this is usually done by those who intend to
  provide a reusable set of functionality for external services or applications Airflow integrates with.
* Using the taskflow API to write tasks
* Relying on the consistent behavior of Airflow objects

One aspect of "public interface" is  extending or using Airflow Python classes and functions. The classes
and functions mentioned below can be relied on to maintain backwards-compatible signatures and behaviours within
MAJOR version of Airflow. On the other hand, classes and methods starting with ``_`` (also known
as protected Python methods) and ``__`` (also known as private Python methods) are not part of the Public
Airflow Interface and might change at any time.

You can also use Airflow's Public Interface via the :doc:`Stable REST API <stable-rest-api-ref>` (based on the
OpenAPI specification). For specific needs you can also use the
:doc:`Airflow Command Line Interface (CLI) <cli-and-env-variables-ref>` though its behaviour might change
in details (such as output format and available flags) so if you want to rely on those in programmatic
way, the Stable REST API is recommended.


Using the Public Interface for Dag authors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The primary interface for Dag authors is the :doc:`airflow.sdk namespace <core-concepts/taskflow>`.
This provides a stable, well-defined interface for creating Dags and tasks that is not subject to internal
implementation changes. The goal of this change is to decouple Dag authoring from Airflow internals (Scheduler,
API Server, etc.), providing a version-agnostic, stable interface for writing and maintaining Dags across Airflow versions.

**Key Imports from airflow.sdk:**

**Classes:**

* ``Asset``
* ``BaseHook``
* ``BaseNotifier``
* ``BaseOperator``
* ``BaseOperatorLink``
* ``BaseSensorOperator``
* ``Connection``
* ``Context``
* ``DAG``
* ``EdgeModifier``
* ``Label``
* ``ObjectStoragePath``
* ``Param``
* ``TaskGroup``
* ``Variable``

**Decorators and Functions:**

* ``@asset``
* ``@dag``
* ``@setup``
* ``@task``
* ``@task_group``
* ``@teardown``
* ``chain``
* ``chain_linear``
* ``cross_downstream``
* ``get_current_context``
* ``get_parsing_context``

.. seealso::
   API reference for :class:`~airflow.sdk.TaskGroup`, :class:`~airflow.sdk.DAG`, and :class:`~airflow.sdk.task_group`

**Migration from Airflow 2.x:**

For detailed migration instructions from Airflow 2.x to 3.x, including import changes and other breaking changes,
see the :doc:`Migration Guide <installation/upgrading_to_airflow3>`.

For an exhaustive list of available classes, decorators, and functions, check ``airflow.sdk.__all__``.

All Dags should update imports to use ``airflow.sdk`` instead of referencing internal Airflow modules directly.
Legacy import paths (e.g., ``airflow.models.dag.DAG``, ``airflow.decorator.task``) are deprecated and will be
removed in a future Airflow version.

Dags
----

The Dag is Airflow's core entity that represents a recurring workflow. You can create a Dag by
instantiating the :class:`~airflow.sdk.DAG` class in your Dag file. Dags can also have parameters
specified via :class:`~airflow.sdk.Param` class.

The recommended way to create Dags is using the :func:`~airflow.sdk.dag` decorator
from the airflow.sdk namespace.

Airflow has a set of example Dags that you can use to learn how to write Dags

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/example_dags/index

You can read more about Dags in :doc:`Dags <core-concepts/dags>`.

References for the modules used in Dags are here:

.. note::
   The airflow.sdk namespace provides the primary interface for Dag authors.
   For detailed API documentation, see the `Task SDK Reference <https://airflow.apache.org/docs/task-sdk/stable/>`_.

.. note::
   The :class:`~airflow.models.dagbag.DagBag` class is used internally by Airflow for loading Dags
   from files and folders. Dag authors should use the :class:`~airflow.sdk.DAG` class from the
   airflow.sdk namespace instead.

.. note::
   The :class:`~airflow.models.dagrun.DagRun` class is used internally by Airflow for Dag run
   management. Dag authors should access Dag run information through the Task Context via
   :func:`~airflow.sdk.get_current_context` or use the :class:`~airflow.sdk.types.DagRunProtocol`
   interface.

.. _pythonapi:operators:

Operators
---------

The base classes :class:`~airflow.sdk.BaseOperator` and :class:`~airflow.sdk.BaseSensorOperator` are public and may be extended to make new operators.

The base class for new operators is :class:`~airflow.sdk.BaseOperator`
from the airflow.sdk namespace.

Subclasses of BaseOperator which are published in Apache Airflow are public in *behavior* but not in *structure*.  That is to say, the Operator's parameters and behavior is governed by semver but the methods are subject to change at any time.

Task Instances
--------------

Task instances are the individual runs of a single task in a Dag (in a Dag Run). Task instances are accessed through
the Task Context via :func:`~airflow.sdk.get_current_context`. Direct database access is not possible.

.. note::
   Task Context is part of the airflow.sdk namespace.
   For detailed API documentation, see the `Task SDK Reference <https://airflow.apache.org/docs/task-sdk/stable/>`_.

Task Instance Keys
------------------

Task instance keys are unique identifiers of task instances in a Dag (in a Dag Run). A key is a tuple that consists of
``dag_id``, ``task_id``, ``run_id``, ``try_number``, and ``map_index``.

Direct access to task instance keys via the :class:`~airflow.models.taskinstance.TaskInstance`
model is no longer allowed from task code. Instead, use the Task Context via :func:`~airflow.sdk.get_current_context`
to access task instance information.

Example of accessing task instance information through Task Context:

.. code-block:: python

    from airflow.sdk import get_current_context


    def my_task():
        context = get_current_context()
        ti = context["ti"]

        dag_id = ti.dag_id
        task_id = ti.task_id
        run_id = ti.run_id
        try_number = ti.try_number
        map_index = ti.map_index

        print(f"Task: {dag_id}.{task_id}, Run: {run_id}, Try: {try_number}, Map Index: {map_index}")

.. note::
   The :class:`~airflow.models.taskinstancekey.TaskInstanceKey` class is used internally by Airflow
   for identifying task instances. Dag authors should access task instance information through the
   Task Context via :func:`~airflow.sdk.get_current_context` instead.


.. _pythonapi:hooks:

Hooks
-----

Hooks are interfaces to external platforms and databases, implementing a common
interface when possible and acting as building blocks for operators. All hooks
are derived from :class:`~airflow.sdk.bases.hook.BaseHook`.

Airflow has a set of Hooks that are considered public. You are free to extend their functionality
by extending them:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/hooks/index

Public Airflow utilities
^^^^^^^^^^^^^^^^^^^^^^^^

When writing or extending Hooks and Operators, Dag authors and developers can
use the following classes:

* The :class:`~airflow.sdk.Connection`, which provides access to external service credentials and configuration.
* The :class:`~airflow.sdk.Variable`, which provides access to Airflow configuration variables.
* The :class:`~airflow.sdk.execution_time.xcom.XCom` which are used to access to inter-task communication data.

Connection and Variable operations should be performed through the Task Context using
:func:`~airflow.sdk.get_current_context` and the task instance's methods, or through the airflow.sdk namespace.
Direct database access to :class:`~airflow.models.connection.Connection` and :class:`~airflow.models.variable.Variable`
models is no longer allowed from task code.

Example of accessing Connections and Variables through Task Context:

.. code-block:: python

    from airflow.sdk import get_current_context


    def my_task():
        context = get_current_context()

        conn = context["conn"]
        my_connection = conn.get("my_connection_id")

        var = context["var"]
        my_variable = var.value.get("my_variable_name")

Example of using airflow.sdk namespace directly:

.. code-block:: python

    from airflow.sdk import Connection, Variable

    conn = Connection.get("my_connection_id")
    var = Variable.get("my_variable_name")

You can read more about the public Airflow utilities in :doc:`howto/connection`,
:doc:`core-concepts/variables`, :doc:`core-concepts/xcoms`


Reference for classes used for the utilities are here:

.. note::
   Connection, Variable, and XCom classes are now part of the airflow.sdk namespace.
   For detailed API documentation, see the `Task SDK Reference <https://airflow.apache.org/docs/task-sdk/stable/>`_.


Public Exceptions
^^^^^^^^^^^^^^^^^

When writing the custom Operators and Hooks, you can handle and raise public Exceptions that Airflow
exposes:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/exceptions/index

Public Utility classes
^^^^^^^^^^^^^^^^^^^^^^

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/utils/state/index


Using Public Interface to extend Airflow capabilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Airflow uses Plugin mechanism to extend Airflow platform capabilities. They allow to extend
Airflow UI but also they are the way to expose the below customizations (Triggers, Timetables, Listeners, etc.).
Providers can also implement plugin endpoints and customize Airflow UI and the customizations.

You can read more about plugins in :doc:`administration-and-deployment/plugins`. You can read how to extend
Airflow UI in :doc:`howto/custom-view-plugin`. Note that there are some simple customizations of the UI
that do not require plugins - you can read more about them in :doc:`howto/customize-ui`.

Here are the ways how Plugins can be used to extend Airflow:

Triggers
--------

Airflow uses Triggers to implement ``asyncio`` compatible Deferrable Operators.
All Triggers derive from :class:`~airflow.triggers.base.BaseTrigger`.

Airflow has a set of Triggers that are considered public. You are free to extend their functionality
by extending them:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/triggers/index

You can read more about Triggers in :doc:`authoring-and-scheduling/deferring`.

Timetables
----------

Custom timetable implementations provide Airflow's scheduler additional logic to
schedule Dag runs in ways not possible with built-in schedule expressions.
All Timetables derive from :class:`~airflow.timetables.base.Timetable`.

Airflow has a set of Timetables that are considered public. You are free to extend their functionality
by extending them:

.. toctree::
  :includehidden:
  :maxdepth: 1

  _api/airflow/timetables/index

You can read more about Timetables in :doc:`howto/timetable`.

Listeners
---------

Listeners enable you to respond to Dag/Task lifecycle events.

This is implemented via :class:`~airflow.listeners.listener.ListenerManager` class that provides hooks that
can be implemented to respond to Dag/Task lifecycle events.

.. versionadded:: 2.5

   Listener public interface has been added in version 2.5.

You can read more about Listeners in :doc:`administration-and-deployment/listeners`.

Extra Links
-----------

Extra links are dynamic links that could be added to Airflow independently from custom Operators. Normally
they can be defined by the Operators, but plugins allow you to override the links on a global level.

You can read more about the Extra Links in :doc:`/howto/define-extra-link`.

Using Public Interface to integrate with external services and applications
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


Tasks in Airflow can orchestrate external services via Hooks and Operators. The core functionality of
Airflow (such as authentication) can also be extended to leverage external services.
You can read more about providers :doc:`providers <apache-airflow-providers:index>` and core
extensions they can provide in :doc:`providers <apache-airflow-providers:core-extensions/index>`.

Executors
---------

Executors are the mechanism by which task instances get run. All executors are
derived from :class:`~airflow.executors.base_executor.BaseExecutor`. There are several
executor implementations built-in Airflow, each with their own unique characteristics and capabilities.

The executor interface itself (the BaseExecutor class) is public, but the built-in executors are not (i.e. KubernetesExecutor, LocalExecutor, etc).  This means that, to use KubernetesExecutor as an example, we may make changes to KubernetesExecutor in minor or patch Airflow releases which could break an executor that subclasses KubernetesExecutor.  This is necessary to allow Airflow developers sufficient freedom to continue to improve the executors we offer.  Accordingly, if you want to modify or extend a built-in executor, you should incorporate the full executor code into your project so that such changes will not break your derivative executor.

You can read more about executors and how to write your own in :doc:`core-concepts/executor/index`.

.. versionadded:: 2.6

  The executor interface has been present in Airflow for quite some time but prior to 2.6, there was executor-specific
  code elsewhere in the codebase.  As of version 2.6 executors are fully decoupled, in the sense that Airflow core no
  longer needs to know about the behavior of specific executors.
  You could have succeeded with implementing a custom executor before Airflow 2.6, and a number
  of people did, but there were some hard-coded behaviours that preferred in-built
  executors, and custom executors could not provide full functionality that built-in executors had.

Secrets Backends
----------------

Airflow can be configured to rely on secrets backends to retrieve
:class:`~airflow.sdk.Connection` and :class:`~airflow.sdk.Variable`.
All secrets backends derive from :class:`~airflow.secrets.base_secrets.BaseSecretsBackend`.

All Secrets Backend implementations are public. You can extend their functionality:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/secrets/index

You can read more about Secret Backends in :doc:`security/secrets/secrets-backend/index`.
You can also find all the available Secrets Backends implemented in community providers
in :doc:`apache-airflow-providers:core-extensions/secrets-backends`.

Auth managers
-------------

Auth managers are responsible of user authentication and user authorization in Airflow. All auth managers are
derived from :class:`~airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager`.

The auth manager interface itself (the :class:`~airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager` class) is
public, but the different implementations of auth managers are not (i.e. FabAuthManager).

You can read more about auth managers and how to write your own in :doc:`core-concepts/auth-manager/index`.

Connections
-----------

When creating Hooks, you can add custom Connections. You can read more
about connections in :doc:`apache-airflow-providers:core-extensions/connections` for available
Connections implemented in the community providers.

Extra Links
-----------

When creating Hooks, you can add custom Extra Links that are displayed when the tasks are run.
You can find out more about extra links in :doc:`apache-airflow-providers:core-extensions/extra-links`
that also shows available extra links implemented in the community providers.

Logging and Monitoring
----------------------

You can extend the way how logs are written by Airflow. You can find out more about log writing in
:doc:`administration-and-deployment/logging-monitoring/index`.

The :doc:`apache-airflow-providers:core-extensions/logging` that also shows available log writers
implemented in the community providers.

Decorators
----------
Dag authors can use decorators to author Dags using the :doc:`TaskFlow <core-concepts/taskflow>` concept.
All Decorators derive from :class:`~airflow.sdk.bases.decorator.TaskDecorator`.

The primary decorators for Dag authors are now in the airflow.sdk namespace:
:func:`~airflow.sdk.dag`, :func:`~airflow.sdk.task`, :func:`~airflow.sdk.asset`,
:func:`~airflow.sdk.setup`, :func:`~airflow.sdk.task_group`, :func:`~airflow.sdk.teardown`,
:func:`~airflow.sdk.chain`, :func:`~airflow.sdk.chain_linear`, :func:`~airflow.sdk.cross_downstream`,
:func:`~airflow.sdk.get_current_context` and :func:`~airflow.sdk.get_parsing_context`.

Airflow has a set of Decorators that are considered public. You are free to extend their functionality
by extending them:

.. note::
   Decorators are now part of the airflow.sdk namespace.
   For detailed API documentation, see the `Task SDK Reference <https://airflow.apache.org/docs/task-sdk/stable/>`_.

You can read more about creating custom Decorators in :doc:`howto/create-custom-decorator`.

Email notifications
-------------------

Airflow has a built-in way of sending email notifications and it allows to extend it by adding custom
email notification classes. You can read more about email notifications in :doc:`howto/email-config`.

Notifications
-------------
Airflow has a built-in extensible way of sending notifications using the various ``on_*_callback``. You can read more
about notifications in :doc:`howto/notifications`.

Cluster Policies
----------------

Cluster Policies are the way to dynamically apply cluster-wide policies to the Dags being parsed or tasks
being executed. You can read more about Cluster Policies in :doc:`administration-and-deployment/cluster-policies`.

Lineage
-------

Airflow can help track origins of data, what happens to it and where it moves over time. You can read more
about lineage in :doc:`administration-and-deployment/lineage`.




What is not part of the Public Interface of Apache Airflow?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Everything not mentioned in this document should be considered as non-Public Interface.

Sometimes in other applications those components could be relied on to keep backwards compatibility,
but in Airflow they are not parts of the Public Interface and might change any time:

* :doc:`Database structure <database-erd-ref>` is considered to be an internal implementation
  detail and you should not assume the structure is going to be maintained in a
  backwards-compatible way.

* :doc:`Web UI <ui>` is continuously evolving and there are no backwards
  compatibility guarantees on HTML elements.

* Python classes except those explicitly mentioned in this document, are considered an
  internal implementation detail and you should not assume they will be maintained
  in a backwards-compatible way.

**Direct metadata database access from task code is no longer allowed**.
Task code cannot directly access the metadata database to query Dag state, task history,
or Dag runs. Instead, use one of the following alternatives:

* **Task Context**: Use :func:`~airflow.sdk.get_current_context` to access task instance
  information and methods like :meth:`~airflow.sdk.types.RuntimeTaskInstanceProtocol.get_dr_count`,
  :meth:`~airflow.sdk.types.RuntimeTaskInstanceProtocol.get_dagrun_state`, and
  :meth:`~airflow.sdk.types.RuntimeTaskInstanceProtocol.get_task_states`.

* **REST API**: Use the :doc:`Stable REST API <stable-rest-api-ref>` for programmatic
  access to Airflow metadata.

* **Python Client**: Use the `Python Client <https://github.com/apache/airflow-client-python>`_ for Python-based
  interactions with Airflow.

This change improves architectural separation and enables remote execution capabilities.

Example of using Task Context instead of direct database access:

.. code-block:: python

    from airflow.sdk import dag, get_current_context, task, DagRunState
    from datetime import datetime


    @dag(dag_id="example_dag", start_date=datetime(2025, 1, 1), schedule="@hourly", tags=["misc"], catchup=False)
    def example_dag():

        @task(task_id="check_dagrun_state")
        def check_state():
            context = get_current_context()
            ti = context["ti"]
            dag_run = context["dag_run"]

            # Use Task Context methods instead of direct DB access
            dr_count = ti.get_dr_count(dag_id="example_dag")
            dagrun_state = ti.get_dagrun_state(dag_id="example_dag", run_id=dag_run.run_id)

            return f"Dag run count: {dr_count}, current state: {dagrun_state}"

        check_state()


    example_dag()
