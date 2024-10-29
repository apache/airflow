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

Public Interface of Airflow
...........................

The Public Interface of Apache Airflow is the collection of interfaces and behaviors in Apache Airflow
whose changes are governed by semantic versioning. A user interacts with Airflow's public interface
by creating and managing Dags, managing tasks and dependencies,
and extending Airflow capabilities by writing new executors, plugins, operators and providers. The
Public Interface can be useful for building custom tools and integrations with other systems,
and for automating certain aspects of the Airflow workflow.

Using Airflow Public Interfaces
===============================

The following are some examples of the public interface of Airflow:

* When you are writing your own operators or hooks. This commonly done when no hook or operator exists for your use case, or when perhaps when one exists but you need to customize the behavior.
* When writing new :doc:`Plugins <authoring-and-scheduling/plugins>` that extend Airflow's functionality beyond
  Dag building blocks. Secrets, Timetables, Triggers, Listeners are all examples of such functionality. This
  is usually done by users who manage Airflow instances.
* Bundling custom Operators, Hooks, Plugins and releasing them together via
  :doc:`provider packages <apache-airflow-providers:index>` - this is usually done by those who intend to
  provide a reusable set of functionality for external services or applications Airflow integrates with.
* Using the taskflow API to write tasks
* Relying on the consistent behavior of Airflow objects

One aspect of "public interface" is  extending or using Airflow Python classes and functions. The classes
and functions mentioned below can be relied on to keep backwards-compatible signatures and behaviours within
MAJOR version of Airflow. On the other hand, classes and methods starting with ``_`` (also known
as protected Python methods) and ``__`` (also known as private Python methods) are not part of the Public
Airflow Interface and might change at any time.

You can also use Airflow's Public Interface via the `Stable REST API <stable-rest-api-ref>`_ (based on the
OpenAPI specification). For specific needs you can also use the
`Airflow Command Line Interface (CLI) <cli-and-env-variables-ref>`_ though its behaviour might change
in details (such as output format and available flags) so if you want to rely on those in programmatic
way, the Stable REST API is recommended.


Using the Public Interface for Dag Authors
==========================================

Dags
----

The Dag is Airflow's core entity that represents a recurring workflow. You can create a Dag by
instantiating the :class:`~airflow.models.dag.DAG` class in your Dag file. You can also instantiate
them via :class:`~airflow.models.dagbag.DagBag` class that reads Dags from a file or a folder. Dags
can also have parameters specified via :class:`~airflow.models.param.Param` class.

Airflow has a set of example Dags that you can use to learn how to write Dags

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/example_dags/index

You can read more about Dags in :doc:`Dags <core-concepts/dags>`.

References for the modules used in Dags are here:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/models/dag/index
  _api/airflow/models/dagbag/index
  _api/airflow/models/param/index

Properties of a :class:`~airflow.models.dagrun.DagRun` can also be referenced in things like :ref:`Templates <templates-ref>`.

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/models/dagrun/index

.. _pythonapi:operators:

Operators
---------

The base classes :class:`~airflow.models.baseoperator.BaseOperator` and :class:`~airflow.sensors.base.BaseSensorOperator` are public and may be extended to make new operators.

Subclasses of BaseOperator which are published in Apache Airflow are public in *behavior* but not in *structure*.  That is to say, the Operator's parameters and behavior is governed by semver but the methods are subject to change at any time.

Task Instances
--------------

Task instances are the individual runs of a single task in a Dag (in a Dag Run). They are available in the context
passed to the execute method of the operators via the :class:`~airflow.models.taskinstance.TaskInstance` class.

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/models/taskinstance/index


Task Instance Keys
------------------

Task instance keys are unique identifiers of task instances in a Dag (in a Dag Run). A key is a tuple that consists of
``dag_id``, ``task_id``, ``run_id``, ``try_number``, and ``map_index``. The key of a task instance can be retrieved via
:meth:`~airflow.models.taskinstance.TaskInstance.key`.

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/models/taskinstancekey/index

.. _pythonapi:hooks:

Hooks
-----

Hooks are interfaces to external platforms and databases, implementing a common
interface when possible and acting as building blocks for operators. All hooks
are derived from :class:`~airflow.hooks.base.BaseHook`.

Airflow has a set of Hooks that are considered public. You are free to extend their functionality
by extending them:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/hooks/index

Public Airflow utilities
------------------------

When writing or extending Hooks and Operators, Dag authors and developers can
use the following classes:

* The :class:`~airflow.models.connection.Connection`, which provides access to external service credentials and configuration.
* The :class:`~airflow.models.variable.Variable`, which provides access to Airflow configuration variables.
* The :class:`~airflow.models.xcom.XCom` which are used to access to inter-task communication data.

You can read more about the public Airflow utilities in :doc:`howto/connection`,
:doc:`core-concepts/variables`, :doc:`core-concepts/xcoms`


Reference for classes used for the utilities are here:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/models/connection/index
  _api/airflow/models/variable/index
  _api/airflow/models/xcom/index


Public Exceptions
-----------------

When writing the custom Operators and Hooks, you can handle and raise public Exceptions that Airflow
exposes:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/exceptions/index

Public Utility classes
----------------------

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/utils/state/index


Using Public Interface to extend Airflow capabilities
=====================================================

Airflow uses Plugin mechanism to extend Airflow platform capabilities. They allow to extend
Airflow UI but also they are the way to expose the below customizations (Triggers, Timetables, Listeners, etc.).
Providers can also implement plugin endpoints and customize Airflow UI and the customizations.

You can read more about plugins in :doc:`authoring-and-scheduling/plugins`. You can read how to extend
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

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  _api/airflow/models/baseoperatorlink/index

You can read more about the Extra Links in :doc:`/howto/define-extra-link`.

Using Public Interface to integrate with external services and applications
===========================================================================


Tasks in Airflow can orchestrate external services via Hooks and Operators. The core functionality of
Airflow (such as authentication) can also be extended to leverage external services.
You can read more about providers :doc:`provider packages <apache-airflow-providers:index>` and core
extensions they can provide in :doc:`provider packages <apache-airflow-providers:core-extensions/index>`.

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
:class:`~airflow.models.connection.Connection` and :class:`~airflow.models.variable.Variable`.
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
derived from :class:`~airflow.auth.managers.base_auth_manager.BaseAuthManager`.

The auth manager interface itself (the :class:`~airflow.auth.managers.base_auth_manager.BaseAuthManager` class) is
public, but the different implementations of auth managers are not (i.e. FabAuthManager).

You can read more about auth managers and how to write your own in :doc:`core-concepts/auth-manager`.

Authentication Backends
-----------------------

Authentication backends can extend the way how Airflow authentication mechanism works. You can find out more
about authentication in :doc:`apache-airflow-providers:core-extensions/auth-backends` that also shows available
Authentication backends implemented in the community providers. In case of authentication backend implemented in a
provider, it is then part of the provider's public interface and not Airflow's.

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
All Decorators derive from :class:`~airflow.decorators.base.TaskDecorator`.

Airflow has a set of Decorators that are considered public. You are free to extend their functionality
by extending them:

.. toctree::
  :includehidden:
  :maxdepth: 1

  _api/airflow/decorators/index

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
===========================================================

Everything not mentioned in this document should be considered as non-Public Interface.

Sometimes in other applications those components could be relied on to keep backwards compatibility,
but in Airflow they are not parts of the Public Interface and might change any time:

* `Database structure <database-erd-ref>`_ is considered to be an internal implementation
  detail and you should not assume the structure is going to be maintained in a
  backwards-compatible way.

* `Web UI <ui>`_ is continuously evolving and there are no backwards compatibility guarantees on HTML elements.

* Python classes except those explicitly mentioned in this document, are considered an
  internal implementation detail and you should not assume they will be maintained
  in a backwards-compatible way.
