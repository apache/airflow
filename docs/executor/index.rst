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

Executors are the mechanism by which task instances get run.

Airflow has support for various executors. Current used is determined by the ``executor`` option in the ``core``
section of the configuration file. This option should contain the name executor e.g. ``KubernetesExecutor``
if it is a core executor. If it is to load your own executor, then you should specify the
full path to the module e.g. ``my_acme_company.executors.MyCustomExecutor``.

.. note::
    For more information on setting the configuration, see :doc:`../howto/set-config`.

If you want to check which executor is currently set, you can use ``airflow config get-value core executor`` command as in
the example below.

.. code-block:: bash

    $ airflow config get-value core executor
    SequentialExecutor

.. toctree::
    :maxdepth: 1

    sequential
    debug
    local
    dask
    celery
    kubernetes
