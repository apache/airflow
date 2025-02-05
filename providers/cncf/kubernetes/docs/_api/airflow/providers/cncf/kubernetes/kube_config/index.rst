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

airflow.providers.cncf.kubernetes.kube_config
=============================================

.. py:module:: airflow.providers.cncf.kubernetes.kube_config


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.kube_config.KubeConfig


Module Contents
---------------

.. py:class:: KubeConfig

   Configuration for Kubernetes.


   .. py:attribute:: core_section
      :value: 'core'



   .. py:attribute:: kubernetes_section
      :value: 'kubernetes_executor'



   .. py:attribute:: logging_section
      :value: 'logging'



   .. py:attribute:: core_configuration


   .. py:attribute:: airflow_home


   .. py:attribute:: dags_folder


   .. py:attribute:: parallelism


   .. py:attribute:: pod_template_file


   .. py:attribute:: delete_worker_pods
      :value: True



   .. py:attribute:: delete_worker_pods_on_failure
      :value: True



   .. py:attribute:: worker_pod_pending_fatal_container_state_reasons
      :value: []



   .. py:attribute:: worker_pods_creation_batch_size


   .. py:attribute:: worker_container_repository


   .. py:attribute:: worker_container_tag


   .. py:attribute:: kube_namespace


   .. py:attribute:: multi_namespace_mode
      :value: True



   .. py:attribute:: executor_namespace


   .. py:attribute:: kube_client_request_args


   .. py:attribute:: delete_option_kwargs
