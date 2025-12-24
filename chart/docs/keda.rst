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

*This feature is still experimental.*

KEDA stands for Kubernetes Event Driven Autoscaling.
`KEDA <https://github.com/kedacore/keda>`__ is a custom controller that
allows users to create custom bindings to the Kubernetes `Horizontal Pod
Autoscaler <https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/>`__.
The autoscaler will adjust the number of active Celery workers based on the number
of tasks in ``queued`` or ``running`` state.

One advantage of KEDA is it allows you to scale your application to/from 0 workers, meaning no workers are idle when there are no tasks.

.. code-block:: bash

   helm repo add kedacore https://kedacore.github.io/charts

   helm repo update

   kubectl create namespace keda

   helm install keda kedacore/keda \
       --namespace keda \
       --version "v2.0.0"

Enable for the Airflow instance by setting ``workers.keda.enabled=true`` in your
helm command or in the ``values.yaml``.

Make sure ``values.yaml`` shows that either KEDA or HPA is enabled, but not both.

It is recommended not to use both a KEDA and an HPA to scale the same workload.
They will compete with each other resulting in odd scaling behavior.

.. code-block:: bash

   kubectl create namespace airflow
   helm repo add apache-airflow https://airflow.apache.org
   helm install airflow apache-airflow/airflow \
       --namespace airflow \
       --set executor=CeleryExecutor \
       --set workers.keda.enabled=true

A ``ScaledObject`` and an ``hpa`` will be created in the Airflow namespace.

KEDA will derive the desired number of Celery workers by querying
Airflow metadata database:

.. code-block:: none

   SELECT
       ceil(COUNT(*)::decimal / {{ .Values.config.celery.worker_concurrency }})
   FROM task_instance
   WHERE state='running' OR state='queued'

.. note::

   Set Celery worker concurrency through the Helm value
   ``config.celery.worker_concurrency`` (i.e. instead of airflow.cfg or
   environment variables) so that the KEDA trigger will be consistent with
   the worker concurrency setting.

Triggers (aka Scalers)
----------------------

Triggers refer to the metrics (or formulae) that KEDA should refer to when scaling workers.

It is recommended to use multiple triggers within a ScaledObject, rather than creating different objects for different triggers.
This keeps all your rules and formulae in one place and it avoids multiple ScaledObjects being spawned by the same workload.


Metrics
-------

The HPA queries your defined triggers according to ``--horizontal-pod-autoscaler-sync-period``.

To reduce the load on the KEDA Scaler, you can pull metrics from a cache by setting ``useCachedMetrics`` to true.

While number of workers = 0, KEDA will still poll for metrics using ``pollingInterval``.
While number of workers >= 1, both KEDA and the HPA will poll your defined triggers.

.. note::
   the HPA refers to ``--horizontal-pod-autoscaler-sync-period`` when polling.

KEDA offers two ``metricTypes`` that provide more granular scaling control than the standard HPA ``Target`` metric.

* AverageValue (default) controls a per-worker average.

* Value controls total system load.

ScaledObject
------------

To configure KEDA's triggers and scaling behaviors, you need to create a ScaledObject.

``targetQueryValue`` is used as ``TargetValue`` of workers, which must be between ``minReplicaCount`` and ``maxReplicaCount``.

``cooldownPeriod`` specifies the number of seconds to wait before downscaling to 0 workers, does not apply to downscaling to n workers while n >= 1.

.. note::
   To avoid strange behavior, it's best practice to set ``cooldownPeriod`` to an integer slightly larger than ``terminationGracePeriodSeconds`` so that your cluster does not downscale to 0 workers before cleanup is finished.

``idleReplicaCount`` can be set to any number less than ``minReplicaCount``, but it must be set to 0, otherwise KEDA will not work.
Change ``minReplicaCount`` to n > 0 if you need idle workers.
