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
---------------------

*This feature is still experimental.*

KEDA stands for Kubernetes Event Driven Autoscaling.
`KEDA <https://github.com/kedacore/keda>`__ is a custom controller that
allows users to create custom bindings to the Kubernetes `Horizontal Pod
Autoscaler <https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/>`__.
We have built scalers that allows users to create scalers based on
PostgreSQL queries and shared it with the community. This enables us to
scale the number of airflow workers deployed on Kubernetes by this chart
depending on the number of task that are ``queued`` or ``running``.

.. code-block:: bash

   helm repo add kedacore https://kedacore.github.io/charts

   helm repo update

   helm install \
       --set image.keda=docker.io/kedacore/keda:1.2.0 \
       --set image.metricsAdapter=docker.io/kedacore/keda-metrics-adapter:1.2.0 \
       --namespace keda --name keda kedacore/keda

Once KEDA is installed (which should be pretty quick since there is only
one pod). You can try out KEDA autoscaling on this chart by setting
``workers.keda.enabled=true`` your helm command or in the
``values.yaml``. (Note: KEDA does not support StatefulSets so you need
to set ``worker.persistence.enabled`` to ``false``)

.. code-block:: bash

   kubectl create namespace airflow

   helm install airflow . \
       --namespace airflow \
       --set executor=CeleryExecutor \
       --set workers.keda.enabled=true \
       --set workers.persistence.enabled=false

KEDA will derive the desired number of celery workers by querying
Airflow metadata database:

.. code-block:: none

   SELECT
       ceil(COUNT(*)::decimal / {{ .Values.config.celery.worker_concurrency }})
   FROM task_instance
   WHERE state='running' OR state='queued'

You should set celery worker concurrency through the helm value
``config.celery.worker_concurrency`` (i.e. instead of airflow.cfg or
environment variables) so that the KEDA trigger will be consistent with
the worker concurrency setting.
