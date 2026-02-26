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

Setting resources for containers
================================

It is possible to set `resources <https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/>`__ for the containers managed by the chart.
You can define different resources for various Airflow containers. By default the resources are not set.

.. note::
    The Kubernetes scheduler can use resources to decide which node to place the pod on. Since a pod resource request/limit is the sum of
    the resource requests/limits for each container in the pod, it is advised to specify resources for each container in the pod.

Possible containers where resources can be configured include:

* Main Airflow containers and their sidecars. You can add the resources for these containers through the following parameters:

   * ``workers.resources``
   * ``workers.logGroomerSidecar.resources``
   * ``workers.kerberosSidecar.resources``
   * ``workers.kerberosInitContainer.resources``
   * ``scheduler.resources``
   * ``scheduler.logGroomerSidecar.resources``
   * ``dags.gitSync.resources``
   * ``apiServer.resources``
   * ``webserver.resources``
   * ``flower.resources``
   * ``dagProcessor.resources``
   * ``dagProcessor.logGroomerSidecar.resources``
   * ``triggerer.resources``
   * ``triggerer.logGroomerSidecar.resources``

* Containers used for Airflow kubernetes jobs or cron jobs. You can add the resources for these containers through the following parameters:

   * ``cleanup.resources``
   * ``createUserJob.resources``
   * ``migrateDatabaseJob.resources``
   * ``databaseCleanup.resources``

* Other containers that can be deployed by the chart. You can add the resources for these containers through the following parameters:

   * ``statsd.resources``
   * ``pgbouncer.resources``
   * ``pgbouncer.metricsExporterSidecar.resources``
   * ``redis.resources``


For example, specifying resources for worker Kerberos sidecar:

.. code-block:: yaml

  workers:
    kerberosSidecar:
      resources:
        limits:
          cpu: 200m
          memory: 256Mi
        requests:
          cpu: 100m
          memory: 128Mi
