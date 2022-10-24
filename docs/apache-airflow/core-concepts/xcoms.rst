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

::

  Note: If the first task run is not succeeded then on every retry task XComs will be cleared to make the task run idempotent.

Custom XCom Backends
--------------------

The XCom system has interchangeable backends, and you can set which backend is being used via the ``xcom_backend`` configuration option.

If you want to implement your own backend, you should subclass :class:`~airflow.models.xcom.BaseXCom`, and override the ``serialize_value`` and ``deserialize_value`` methods.

There is also an ``orm_deserialize_value`` method that is called whenever the XCom objects are rendered for UI or reporting purposes; if you have large or expensive-to-retrieve values in your XComs, you should override this method to avoid calling that code (and instead return a lighter, incomplete representation) so the UI remains responsive.

You can also override the ``clear`` method and use it when clearing results for given dags and tasks. This allows the custom XCom backend to process the data lifecycle easier.

Working with Custom XCom Backends in Containers
-----------------------------------------------

Depending on where Airflow is deployed i.e., local, Docker, K8s, etc. it can be useful to be assured that a custom XCom backend is actually being initialized. For example, the complexity of the container environment can make it more difficult to determine if your backend is being loaded correctly during container deployment. Luckily the following guidance can be used to assist you in building confidence in your custom XCom implementation.

Firstly, if you can exec into a terminal in the container then you should be able to do:

.. code-block:: python

    from airflow.models.xcom import XCom

    print(XCom.__name__)

which will print the actual class that is being used.

You can also examine Airflow's configuration:

.. code-block:: python

    from airflow.settings import conf

    conf.get("core", "xcom_backend")

Working with Custom Backends in K8s via Helm
--------------------------------------------

Running custom XCom backends in K8s will introduce even more complexity to you Airflow deployment. Put simply, sometimes things go wrong which can be difficult to debug.

For example, if you define a custom XCom backend in the Chart ``values.yaml`` (via the ``xcom_backend`` configuration) and Airflow fails to load the class, the entire Chart deployment will fail with each pod container attempting to restart time and time again.

When deploying in K8s your custom XCom backend needs to be reside in a ``config`` directory otherwise it cannot be located during Chart deployment.

An observed problem is that it is very difficult to acquire logs from the container because there is a very small window of availability where the trace can be obtained. The only way you can determine the root cause is if you are fortunate enough to query and acquire the container logs at the right time. This in turn prevents the entire Helm chart from deploying successfully.
