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

airflow.providers.cncf.kubernetes.kube_client
=============================================

.. py:module:: airflow.providers.cncf.kubernetes.kube_client

.. autoapi-nested-parse::

   Client for kubernetes communication.



Attributes
----------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.kube_client.log
   airflow.providers.cncf.kubernetes.kube_client.has_kubernetes


Functions
---------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.kube_client.get_kube_client


Module Contents
---------------

.. py:data:: log

.. py:data:: has_kubernetes
   :value: True


.. py:function:: get_kube_client(in_cluster = None, cluster_context = None, config_file = None)

   Retrieve Kubernetes client.

   :param in_cluster: whether we are in cluster
   :param cluster_context: context of the cluster
   :param config_file: configuration file
   :return: kubernetes client
