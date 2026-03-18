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

Autoscaling with KEDA
=====================

KEDA stands for Kubernetes Event Driven Autoscaling.
`KEDA <https://github.com/kedacore/keda>`__ is a custom controller that
allows users to create custom bindings to the Kubernetes `Horizontal Pod
Autoscaler <https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/>`__.
The autoscaler will adjust the number of active Celery workers based on the number
of tasks in ``queued`` or ``running`` state.

One advantage of KEDA is that it allows you to scale your application to/from 0 workers, meaning no workers are idle when there are no tasks.

KEDA Installation and usage
---------------------------

To install KEDA in your Kubernetes cluster, run the following commands:

.. code-block:: bash

   helm repo add kedacore https://kedacore.github.io/charts
   helm repo update
   kubectl create namespace keda
   helm install keda kedacore/keda \
     --namespace keda \
     --version "v2.0.0"

To enable KEDA for the Airflow instance, it has to be enabled by setting ``workers.celery.keda.enabled=true``
in your Helm command or in the ``values.yaml`` like:

.. code-block:: bash

   kubectl create namespace airflow
   helm repo add apache-airflow https://airflow.apache.org
   helm install airflow apache-airflow/airflow \
     --namespace airflow \
     --set executor=CeleryExecutor \
     --set workers.celery.keda.enabled=true

.. note::

   Make sure ``values.yaml`` shows that either KEDA or HPA is enabled, but not both. It is recommended not
   to use both KEDA and HPA to scale the same workload. They will compete with each other resulting in odd scaling behavior.

After installation, the KEDA ``ScaledObject`` and an ``HPA`` will be created in the Airflow namespace.

In the default configuration, KEDA will derive the desired number of Celery workers by querying Airflow metadata database with following SQL statement:

.. code-block:: none

   SELECT
     ceil(COUNT(*)::decimal / {{ .Values.config.celery.worker_concurrency }})
   FROM
     task_instance
   WHERE
     (state='running' OR state='queued')
     AND queue IN <queue names>

where ``<queue names>`` is a list of queue names used by
`Celery worker queues <https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html#queues>`_
mechanism (with default configuration it has one element ``default``).

.. note::

   Set Celery worker concurrency through the Helm Chart value
   ``config.celery.worker_concurrency`` (e.g. instead of airflow.cfg or
   environment variables), so that the KEDA trigger will be consistent with
   the worker concurrency setting.

Triggers (aka Scalers)
----------------------

Triggers refer to the metrics (or formulae) that KEDA should refer to when scaling workers.

It is recommended to use multiple triggers within a ScaledObject, rather than creating different objects for different triggers.
This keeps all your rules and formulae in one place, and it avoids multiple ScaledObjects being created by the same workload.

ScaledObject
------------

To configure KEDA's triggers and scaling behaviors, you need to create a ScaledObject. Below ScaledObject parameters:

* ``cooldownPeriod`` specifies the number of seconds to wait before downscaling to 0 workers, does not apply to downscaling to n workers while n >= 1.
* ``idleReplicaCount`` can be set to any number less than ``minReplicaCount``, but it must be set to 0, otherwise KEDA will not work. Change ``minReplicaCount`` to n > 0 if you need idle workers.

Triggerers value ``targetQueryValue`` is used as ``TargetValue`` of workers, which must be between ScaledObject ``minReplicaCount`` and ``maxReplicaCount`` values.

.. note::

   To avoid strange behavior, best practice is to set ``cooldownPeriod`` to an integer slightly larger than ``terminationGracePeriodSeconds`` so that your cluster does not downscale to 0 workers before cleanup is finished.

Metrics
-------

The HPA controller, refreshes metrics defined in triggers every ``--horizontal-pod-autoscaler-sync-period`` and the values are routed to
KEDA Metrics Server directly. To reduce the load on the KEDA Scaler, you can set ``useCachedMetrics`` to true, to enabling reading metrics
from cache first. Cache is updated periodically every ``pollingInterval``.

.. note::

   When number of workers = 0, KEDA will still poll for metrics using ``pollingInterval``.
   When number of workers >= 1, both KEDA and the HPA will poll your defined triggers.

KEDA offers two ``metricTypes`` that provide more granular scaling control than the standard HPA ``Target`` metric:

* AverageValue (default) controls a per-worker average.
* Value controls total system load.
