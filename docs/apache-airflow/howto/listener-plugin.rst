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


Listener Plugin of Airflow
==========================

Airflow has feature that allows to add listener for monitoring and tracking
the task state using Plugins.

This is a simple example listener plugin of Airflow that helps to track the task
state and collect useful metadata information about the task, dag run and dag.

This is an example plugin for Airflow that allows to create listener plugin of Airflow.
This plugin works by using SQLAlchemy's event mechanism. It watches
the task instance state change in the table level and triggers event.
This will be notified for all the tasks across all the DAGs.

In this plugin, an object reference is derived from the base class
``airflow.plugins_manager.AirflowPlugin``.

Listener plugin uses pluggy app under the hood. Pluggy is an app built for plugin
management and hook calling for Pytest. Pluggy enables function hooking so it allows
building "pluggable" systems with your own customization over that hooking.

Using this plugin, following events can be listened:
    * task instance is in running state.
    * task instance is in success state.
    * task instance is in failure state.
    * dag run is in running state.
    * dag run is in success state.
    * dag run is in failure state.
    * on start before event like airflow job, scheduler or backfilljob
    * before stop for event like airflow job, scheduler or backfilljob

Listener Registration
---------------------

A listener plugin with object reference to listener object is registered
as part of airflow plugin. The following is a
skeleton for us to implement a new listener:

.. code-block:: python

    from airflow.plugins_manager import AirflowPlugin

    # This is the listener file created where custom code to monitor is added over hookimpl
    import listener


    class MetadataCollectionPlugin(AirflowPlugin):
        name = "MetadataCollectionPlugin"
        listeners = [listener]


Next, we can check code added into ``listener`` and see implementation
methods for each of those listeners. After the implementation, the listener part
gets executed during all the task execution across all the DAGs

For reference, here's the plugin code within ``listener.py`` class that shows list of tables in the database:

This example listens when the task instance is in running state

.. exampleinclude:: ../../../airflow/example_dags/plugins/event_listener.py
    :language: python
    :start-after: [START howto_listen_ti_running_task]
    :end-before: [END howto_listen_ti_running_task]

Similarly, code to listen after task_instance success and failure can be implemented.

This example listens when the dag run is change to failed state

.. exampleinclude:: ../../../airflow/example_dags/plugins/event_listener.py
    :language: python
    :start-after: [START howto_listen_dagrun_failure_task]
    :end-before: [END howto_listen_dagrun_failure_task]

Similarly, code to listen after dag_run success and during running state can be implemented.

The listener plugin files required to add the listener implementation is added as part of the
Airflow plugin into ``$AIRFLOW_HOME/plugins/`` folder and loaded during Airflow startup.
