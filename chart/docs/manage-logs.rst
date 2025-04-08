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

Manage logs
=================

You have a number of options when it comes to managing your Airflow logs.

No persistence
-----------------

With this option, Airflow will log locally to each pod. As such, the logs will only be available during the lifetime of the pod.

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set logs.persistence.enabled=false
      # --set workers.persistence.enabled=false (also needed if using ``CeleryExecutor``)

Celery worker log persistence
-----------------------------

If you are using ``CeleryExecutor``, workers persist logs by default to a volume claim created with a ``volumeClaimTemplate``.

You can modify the template:

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set executor=CeleryExecutor \
      --set workers.persistence.size=10Gi

Note with this option only task logs are persisted, unlike when log persistence is enabled which will also persist scheduler logs.

Log persistence enabled
-----------------------

This option will provision a ``PersistentVolumeClaim`` with an access mode of ``ReadWriteMany``. Each component of Airflow will
then log onto the same volume.

Not all volume plugins have support for ``ReadWriteMany`` access mode.
Refer `Persistent Volume Access Modes <https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes>`__
for details.

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set logs.persistence.enabled=true
      # you can also override the other persistence
      # by setting the logs.persistence.* values
      # Please refer to values.yaml for details

Externally provisioned PVC
--------------------------

In this approach, Airflow will log to an existing ``ReadWriteMany`` PVC. You pass in the name of the volume claim to the chart.

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set logs.persistence.enabled=true \
      --set logs.persistence.existingClaim=my-volume-claim

Note that the volume will need to be writable by the Airflow user. The easiest way is to ensure GID ``0`` has write permission.
More information can be found in the :ref:`Docker image entrypoint documentation <docker-stack:arbitrary-docker-user>`.

Elasticsearch
-------------

If your cluster forwards logs to Elasticsearch, you can configure Airflow to retrieve task logs from it.
See the :doc:`Elasticsearch providers guide <apache-airflow-providers-elasticsearch:logging/index>` for more details.

.. code-block:: bash

    helm upgrade --install airflow apache-airflow/airflow \
      --set elasticsearch.enabled=true \
      --set elasticsearch.secretName=my-es-secret
      # Other choices exist. Please refer to values.yaml for details.
