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

Listeners
=========

You can write listeners to enable Airflow to notify you when events happen.
`Pluggy <https://pluggy.readthedocs.io/en/stable/>`__ powers these listeners.

.. warning::

    Listeners are an advanced feature of Airflow. They are not isolated from the Airflow components they run in, and
    can slow down or in come cases take down your Airflow instance. As such, extra care should be taken when writing listeners.

Airflow supports notifications for the following events:

Lifecycle Events
----------------

- ``on_starting``
- ``before_stopping``

Lifecycle events allow you to react to start and stop events for an Airflow ``Job``, like  ``SchedulerJob``.

DagRun State Change Events
--------------------------

DagRun state change events occur when a :class:`~airflow.models.dagrun.DagRun` changes state.
Beginning with Airflow 3, listeners are also notified whenever a state change is triggered through the API
(for ``on_dag_run_success`` and ``on_dag_run_failed``) e.g., when a DagRun is marked as success from the Airflow UI.

- ``on_dag_run_running``

.. exampleinclude:: /../src/airflow/example_dags/plugins/event_listener.py
    :language: python
    :start-after: [START howto_listen_dagrun_running_task]
    :end-before: [END howto_listen_dagrun_running_task]

- ``on_dag_run_success``

.. exampleinclude:: /../src/airflow/example_dags/plugins/event_listener.py
    :language: python
    :start-after: [START howto_listen_dagrun_success_task]
    :end-before: [END howto_listen_dagrun_success_task]

- ``on_dag_run_failed``

.. exampleinclude:: /../src/airflow/example_dags/plugins/event_listener.py
    :language: python
    :start-after: [START howto_listen_dagrun_failure_task]
    :end-before: [END howto_listen_dagrun_failure_task]


TaskInstance State Change Events
--------------------------------

TaskInstance state change events occur when a :class:`~airflow.sdk.execution_time.task_runner.RuntimeTaskInstance` changes state.
You can use these events to react to ``LocalTaskJob`` state changes.
Starting with Airflow 3, listeners are also notified when a state change is triggered through the API
(for ``on_task_instance_success`` and ``on_task_instance_failed``) e.g., when marking a task instance as success from the Airflow UI.
In such cases, the listener will receive a :class:`~airflow.models.taskinstance.TaskInstance` instance instead
of a :class:`~airflow.sdk.execution_time.task_runner.RuntimeTaskInstance` instance.

- ``on_task_instance_running``

.. exampleinclude:: /../src/airflow/example_dags/plugins/event_listener.py
    :language: python
    :start-after: [START howto_listen_ti_running_task]
    :end-before: [END howto_listen_ti_running_task]

- ``on_task_instance_success``

.. exampleinclude:: /../src/airflow/example_dags/plugins/event_listener.py
    :language: python
    :start-after: [START howto_listen_ti_success_task]
    :end-before: [END howto_listen_ti_success_task]

- ``on_task_instance_failed``

.. exampleinclude:: /../src/airflow/example_dags/plugins/event_listener.py
    :language: python
    :start-after: [START howto_listen_ti_failure_task]
    :end-before: [END howto_listen_ti_failure_task]


Asset Events
--------------

- ``on_asset_created``
- ``on_asset_alias_created``
- ``on_asset_changed``

Asset events occur when Asset management operations are run.


Dag Import Error Events
-----------------------

- ``on_new_dag_import_error``
- ``on_existing_dag_import_error``

Dag import error events occur when Dag processor finds import error in the Dag code and update the metadata database table.


|experimental|


Usage
-----

To create a listener:

- import ``airflow.listeners.hookimpl``
- implement the ``hookimpls`` for events that you'd like to generate notifications

Airflow defines the specification as `hookspec <https://github.com/apache/airflow/tree/main/airflow-core/src/airflow/listeners/spec>`__. Your implementation must accept the same named parameters as defined in hookspec. If you don't use the same parameters as hookspec, Pluggy throws an error when you try to use your plugin. But you don't need to implement every method. Many listeners only implement one method, or a subset of methods.

To include the listener in your Airflow installation, include it as a part of an :doc:`Airflow Plugin </administration-and-deployment/plugins>`.

Listener API is meant to be called across all Dags and all operators. You can't listen to events generated by specific Dags. For that behavior, try methods like ``on_success_callback`` and ``pre_execute``. These provide callbacks for particular Dag authors or operator creators. The logs and ``print()`` calls will be handled as part of the listeners.


Compatibility note
------------------

The listeners interface might change over time. We are using ``pluggy`` specifications which
means that implementation of the listeners written for older versions of the interface should be
forward-compatible with future versions of Airflow.

However, the opposite is not guaranteed, so if your listener is implemented against a newer version of the
interface, it might not work with older versions of Airflow. It is not a problem if you target single version
of Airflow, because you can adjust your implementation to the version of Airflow you use, but it is important
if you are writing plugins or extensions that could be used with different versions of Airflow.

For example if a new field is added to the interface (like the ``error`` field in the
``on_task_instance_failed`` method in 2.10.0), the listener implementation will not handle the case when
the field is not present in the event object and such listeners will only work for Airflow 2.10.0 and later.

In order to implement a listener that is compatible with multiple versions of Airflow including using features
and fields added in newer versions of Airflow, you should check version of Airflow used and use newer version
of the interface implementation, but for older versions of Airflow you should use older version of the
interface.

For example if you want to implement a listener that uses the ``error`` field in the
``on_task_instance_failed``, you should use code like this:

.. code-block:: python

    from importlib.metadata import version
    from packaging.version import Version
    from airflow.listeners import hookimpl

    airflow_version = Version(version("apache-airflow"))
    if airflow_version >= Version("2.10.0"):

        class ClassBasedListener:
            ...

            @hookimpl
            def on_task_instance_failed(self, previous_state, task_instance, error: None | str | BaseException):
                # Handle error case here
                pass

    else:

        class ClassBasedListener:  # type: ignore[no-redef]
            ...

            @hookimpl
            def on_task_instance_failed(self, previous_state, task_instance):
                # Handle no error case here
                pass

List of changes in the listener interfaces since 2.8.0 when they were introduced:


+-----------------+--------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------+
| Airflow Version | Affected method                            | Change                                                                                                                        |
+=================+============================================+===============================================================================================================================+
| 2.10.0          | ``on_task_instance_failed``                | An error field added to the interface                                                                                         |
+-----------------+--------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------+
| 3.0.0           | ``on_task_instance_running``               | ``session`` argument removed from task instance listeners,                                                                    |
|                 |                                            | ``task_instance`` object is now an instance of ``RuntimeTaskInstance``                                                        |
+-----------------+--------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------+
| 3.0.0           | ``on_task_instance_failed``,               | ``session`` argument removed from task instance listeners,                                                                    |
|                 | ``on_task_instance_success``               | ``task_instance`` object is now an instance of ``RuntimeTaskInstance`` when on worker and ``TaskInstance`` when on API server |
+-----------------+--------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------+
