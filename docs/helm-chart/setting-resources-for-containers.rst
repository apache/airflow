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
--------------------------------

It is possible to set `resources <https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/>`__ for the Containers managed by the chart. You can define different resources for various Airflow k8s Containers. By default the resources are not set.

.. note::
    The k8s scheduler can use resources to decide which node to place the Pod on. Since a Pod resource request/limit is the sum of the resource requests/limits for each Container in the Pod, it is advised to specify resources for each Container in the Pod.

Possible Containers where resources can be configured include:

* Main Airflow Containers and their sidecars. You can add the resources for these Containers through the following parameters:

   * ``workers.resources``
   * ``workers.logGroomerSidecar.resources``
   * ``workers.kerberosSidecar.resources``
   * ``scheduler.resources``
   * ``scheduler.logGroomerSidecar.resources``
   * ``dags.gitSync.resources``
   * ``webserver.resources``
   * ``flower.resources``
   * ``dagProcessor.resources``
   * ``triggerer.resources``

* Containers used for Airflow k8s jobs or cron jobs. You can add the resources for these Containers through the following parameters:

   * ``cleanup.resources``
   * ``createUserJob.resources``
   * ``migrateDatabaseJob.resources``

* Other containers that can be deployed by the chart. You can add the resources for these Containers through the following parameters:

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
