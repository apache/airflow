Scaling Out with Dask
=====================

``DaskExecutor`` allows you to run Airflow tasks in a Dask Distributed cluster.

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

Edit your ``airflow.cfg`` to set your executor to ``DaskExecutor`` and provide
the Dask Scheduler address in the ``[dask]`` section.

Please note:

- Each Dask worker must be able to import Airflow and any dependencies you
  require.
- Dask does not support queues. If an Airflow task was created with a queue, a
  warning will be raised but the task will be submitted to the cluster.
