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

airflow.providers.cncf.kubernetes.secret
========================================

.. py:module:: airflow.providers.cncf.kubernetes.secret

.. autoapi-nested-parse::

   Classes for interacting with Kubernetes API.



Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.secret.Secret


Module Contents
---------------

.. py:class:: Secret(deploy_type, deploy_target, secret, key=None, items=None)

   Bases: :py:obj:`airflow.providers.cncf.kubernetes.k8s_model.K8SModel`


   Defines Kubernetes Secret Volume.


   .. py:attribute:: deploy_type


   .. py:attribute:: deploy_target


   .. py:attribute:: items
      :value: []



   .. py:attribute:: secret


   .. py:attribute:: key
      :value: None



   .. py:method:: to_env_secret()

      Store es environment secret.



   .. py:method:: to_env_from_secret()

      Read from environment to secret.



   .. py:method:: to_volume_secret()

      Convert to volume secret.



   .. py:method:: attach_to_pod(pod)

      Attach to pod.



   .. py:method:: __eq__(other)


   .. py:method:: __repr__()
