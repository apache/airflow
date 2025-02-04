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

airflow.providers.cncf.kubernetes.sensors.spark_kubernetes
==========================================================

.. py:module:: airflow.providers.cncf.kubernetes.sensors.spark_kubernetes


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.sensors.spark_kubernetes.SparkKubernetesSensor


Module Contents
---------------

.. py:class:: SparkKubernetesSensor(*, application_name, attach_log = False, namespace = None, container_name = 'spark-kubernetes-driver', kubernetes_conn_id = 'kubernetes_default', api_group = 'sparkoperator.k8s.io', api_version = 'v1beta2', **kwargs)

   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`


   Checks sparkApplication object in kubernetes cluster.

   .. seealso::
       For more detail about Spark Application Object have a look at the reference:
       https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication

   :param application_name: spark Application resource name
   :param namespace: the kubernetes namespace where the sparkApplication reside in
   :param container_name: the kubernetes container name where the sparkApplication reside in
   :param kubernetes_conn_id: The :ref:`kubernetes connection<howto/connection:kubernetes>`
       to Kubernetes cluster.
   :param attach_log: determines whether logs for driver pod should be appended to the sensor log
   :param api_group: kubernetes api group of sparkApplication
   :param api_version: kubernetes api version of sparkApplication


   .. py:attribute:: template_fields
      :type:  collections.abc.Sequence[str]
      :value: ('application_name', 'namespace')



   .. py:attribute:: FAILURE_STATES
      :value: ('FAILED', 'UNKNOWN')



   .. py:attribute:: SUCCESS_STATES
      :value: ('COMPLETED',)



   .. py:attribute:: application_name


   .. py:attribute:: attach_log
      :value: False



   .. py:attribute:: namespace
      :value: None



   .. py:attribute:: container_name
      :value: 'spark-kubernetes-driver'



   .. py:attribute:: kubernetes_conn_id
      :value: 'kubernetes_default'



   .. py:attribute:: api_group
      :value: 'sparkoperator.k8s.io'



   .. py:attribute:: api_version
      :value: 'v1beta2'



   .. py:property:: hook
      :type: airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook



   .. py:method:: poke(context)

      Override when deriving this class.
