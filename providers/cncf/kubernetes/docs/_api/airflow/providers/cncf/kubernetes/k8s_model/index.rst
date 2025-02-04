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

airflow.providers.cncf.kubernetes.k8s_model
===========================================

.. py:module:: airflow.providers.cncf.kubernetes.k8s_model

.. autoapi-nested-parse::

   Classes for interacting with Kubernetes API.



Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.k8s_model.K8SModel


Functions
---------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.k8s_model.append_to_pod


Module Contents
---------------

.. py:class:: K8SModel

   Bases: :py:obj:`abc.ABC`


   Airflow Kubernetes models are here for backwards compatibility reasons only.

   Ideally clients should use the kubernetes API
   and the process of

       client input -> Airflow k8s models -> k8s models

   can be avoided. All of these models implement the
   ``attach_to_pod`` method so that they integrate with the kubernetes client.


   .. py:method:: attach_to_pod(pod)
      :abstractmethod:


      Attaches to pod.

      :param pod: A pod to attach this Kubernetes object to
      :return: The pod with the object attached



.. py:function:: append_to_pod(pod, k8s_objects)

   Attach additional specs to an existing pod object.

   :param pod: A pod to attach a list of Kubernetes objects to
   :param k8s_objects: a potential None list of K8SModels
   :return: pod with the objects attached if they exist
