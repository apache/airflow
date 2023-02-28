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

Executor
========

Executors are the mechanism by which :doc:`task instances </core-concepts/tasks>` get run. They have a common API and are "pluggable", meaning you can swap executors based on your installation needs.

Airflow can only have one executor configured at a time; this is set by the ``executor`` option in the ``[core]``
section of :doc:`the configuration file </howto/set-config>`.

Built-in executors are referred to by name, for example:

.. code-block:: ini

    [core]
    executor = KubernetesExecutor

You can also write your own custom executors, and refer to them by their full path:

.. code-block:: ini

    [core]
    executor = my_company.executors.MyCustomExecutor

.. note::
    For more information on Airflow's configuration, see :doc:`/howto/set-config`.

If you want to check which executor is currently set, you can use the ``airflow config get-value core executor`` command:

.. code-block:: bash

    $ airflow config get-value core executor
    SequentialExecutor


Executor Types
--------------

There are two types of executor - those that run tasks *locally* (inside the ``scheduler`` process), and those that run their tasks *remotely* (usually via a pool of *workers*). Airflow comes configured with the ``SequentialExecutor`` by default, which is a local executor, and the safest option for execution, but we *strongly recommend* you change this to ``LocalExecutor`` for small, single-machine installations, or one of the remote executors for a multi-machine/cloud installation.


**Local Executors**

.. toctree::
    :maxdepth: 1

    debug
    local
    sequential

**Remote Executors**

.. toctree::
    :maxdepth: 1

    celery
    celery_kubernetes
    dask
    kubernetes
    local_kubernetes


.. note::

    Something that often confuses new users of Airflow is that they don't need to run a separate ``executor`` process. This is because the executor's logic runs *inside* the ``scheduler`` process - if you're running a scheduler, you're running the executor.
