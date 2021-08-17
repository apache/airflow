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


.. _executor:DaskExecutor:

Dask Executor
=============

:class:`airflow.executors.dask_executor.DaskExecutor` allows you to run Airflow tasks in a Dask Distributed cluster.

Dask clusters can be run on a single machine or on remote networks. For complete
details, consult the `Distributed documentation <https://distributed.readthedocs.io/>`_.

To create a cluster, first start a Scheduler:

.. code-block:: bash

    # default settings for a local cluster
    DASK_HOST=127.0.0.1
    DASK_PORT=8786

    dask-scheduler --host $DASK_HOST --port $DASK_PORT

Next start at least one Worker on any machine that can connect to the host:

.. code-block:: bash

    dask-worker $DASK_HOST:$DASK_PORT

Edit your ``airflow.cfg`` to set your executor to :class:`airflow.executors.dask_executor.DaskExecutor` and provide
the Dask Scheduler address in the ``[dask]`` section. For more information on setting the configuration,
see :doc:`../howto/set-config`.

Please note:

- Each Dask worker must be able to import Airflow and any dependencies you
  require.
- Dask does not support queues. If an Airflow task was created with a queue, a
  warning will be raised but the task will be submitted to the cluster.
