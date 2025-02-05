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

airflow.providers.cncf.kubernetes.utils.xcom_sidecar
====================================================

.. py:module:: airflow.providers.cncf.kubernetes.utils.xcom_sidecar

.. autoapi-nested-parse::

   Attach a sidecar container that blocks the pod from completing until Airflow pulls result data.



Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.utils.xcom_sidecar.PodDefaults


Functions
---------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.utils.xcom_sidecar.add_xcom_sidecar


Module Contents
---------------

.. py:class:: PodDefaults

   Static defaults for Pods.


   .. py:attribute:: XCOM_MOUNT_PATH
      :value: '/airflow/xcom'



   .. py:attribute:: SIDECAR_CONTAINER_NAME
      :value: 'airflow-xcom-sidecar'



   .. py:attribute:: XCOM_CMD
      :value: 'trap "exit 0" INT; while true; do sleep 1; done;'



   .. py:attribute:: VOLUME_MOUNT


   .. py:attribute:: VOLUME


   .. py:attribute:: SIDECAR_CONTAINER


.. py:function:: add_xcom_sidecar(pod, *, sidecar_container_image = None, sidecar_container_resources = None)

   Add sidecar.
