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


.. _concepts:xcom:

XComs
=====

XComs (short for "cross-communications") are a mechanism that let :doc:`tasks` talk to each other, as by default Tasks are entirely isolated and may be running on entirely different machines.

An XCom is identified by a ``key`` (essentially its name), as well as the ``task_id`` and ``dag_id`` it came from. They can have any (serializable) value, but they are only designed for small amounts of data; do not use them to pass around large values, like dataframes.

XComs are explicitly "pushed" and "pulled" to/from their storage using the ``xcom_push`` and ``xcom_pull`` methods on Task Instances. Many operators will auto-push their results into an XCom key called ``return_value`` if the ``do_xcom_push`` argument is set to ``True`` (as it is by default), and ``@task`` functions do this as well.

``xcom_pull`` defaults to using this key if no key is passed to it, meaning it's possible to write code like this::

    # Pulls the return_value XCOM from "pushing_task"
    value = task_instance.xcom_pull(task_ids='pushing_task')

You can also use XComs in :ref:`templates <concepts:jinja-templating>`::

    SELECT * FROM {{ task_instance.xcom_pull(task_ids='foo', key='table_name') }}

XComs are a relative of :doc:`variables`, with the main difference being that XComs are per-task-instance and designed for communication within a DAG run, while Variables are global and designed for overall configuration and value sharing.


Custom Backends
---------------

The XCom system has interchangeable backends, and you can set which backend is being used via the ``xcom_backend`` configuration option.

If you want to implement your own backend, you should subclass :class:`~airflow.models.xcom.BaseXCom`, and override the ``serialize_value`` and ``deserialize_value`` methods.

There is also an ``orm_deserialize_value`` method that is called whenever the XCom objects are rendered for UI or reporting purposes; if you have large or expensive-to-retrieve values in your XComs, you should override this method to avoid calling that code (and instead return a lighter, incomplete representation) so the UI remains responsive.
