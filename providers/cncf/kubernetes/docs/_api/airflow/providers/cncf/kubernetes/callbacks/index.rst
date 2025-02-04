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

airflow.providers.cncf.kubernetes.callbacks
===========================================

.. py:module:: airflow.providers.cncf.kubernetes.callbacks


Attributes
----------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.callbacks.client_type


Classes
-------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.callbacks.ExecutionMode
   airflow.providers.cncf.kubernetes.callbacks.KubernetesPodOperatorCallback


Module Contents
---------------

.. py:data:: client_type

.. py:class:: ExecutionMode

   Bases: :py:obj:`str`, :py:obj:`enum.Enum`


   Enum class for execution mode.


   .. py:attribute:: SYNC
      :value: 'sync'



   .. py:attribute:: ASYNC
      :value: 'async'



.. py:class:: KubernetesPodOperatorCallback

   ``KubernetesPodOperator`` callbacks methods.

   Currently, the callbacks methods are not called in the async mode, this support will be added
   in the future.


   .. py:method:: on_sync_client_creation(*, client, operator, **kwargs)
      :staticmethod:


      Invoke this callback after creating the sync client.

      :param client: the created `kubernetes.client.CoreV1Api` client.



   .. py:method:: on_pod_manifest_created(*, pod_request, client, mode, operator, context, **kwargs)
      :staticmethod:


      Invoke this callback after KPO creates the V1Pod manifest but before the pod is created.

      :param pod_request: the kubernetes pod manifest
      :param client: the Kubernetes client that can be used in the callback.
      :param mode: the current execution mode, it's one of (`sync`, `async`).



   .. py:method:: on_pod_creation(*, pod, client, mode, operator, context, **kwargs)
      :staticmethod:


      Invoke this callback after creating the pod.

      :param pod: the created pod.
      :param client: the Kubernetes client that can be used in the callback.
      :param mode: the current execution mode, it's one of (`sync`, `async`).



   .. py:method:: on_pod_starting(*, pod, client, mode, operator, context, **kwargs)
      :staticmethod:


      Invoke this callback when the pod starts.

      :param pod: the started pod.
      :param client: the Kubernetes client that can be used in the callback.
      :param mode: the current execution mode, it's one of (`sync`, `async`).



   .. py:method:: on_pod_completion(*, pod, client, mode, operator, context, **kwargs)
      :staticmethod:


      Invoke this callback when the pod completes.

      :param pod: the completed pod.
      :param client: the Kubernetes client that can be used in the callback.
      :param mode: the current execution mode, it's one of (`sync`, `async`).



   .. py:method:: on_pod_teardown(*, pod, client, mode, operator, context, **kwargs)
      :staticmethod:


      Invoke this callback after all pod completion callbacks but before the pod is deleted.

      :param pod: the completed pod.
      :param client: the Kubernetes client that can be used in the callback.
      :param mode: the current execution mode, it's one of (`sync`, `async`).



   .. py:method:: on_pod_cleanup(*, pod, client, mode, operator, context, **kwargs)
      :staticmethod:


      Invoke this callback after cleaning/deleting the pod.

      :param pod: the completed pod.
      :param client: the Kubernetes client that can be used in the callback.
      :param mode: the current execution mode, it's one of (`sync`, `async`).



   .. py:method:: on_operator_resuming(*, pod, event, client, mode, operator, context, **kwargs)
      :staticmethod:


      Invoke this callback when resuming the `KubernetesPodOperator` from deferred state.

      :param pod: the current state of the pod.
      :param event: the returned event from the Trigger.
      :param client: the Kubernetes client that can be used in the callback.
      :param mode: the current execution mode, it's one of (`sync`, `async`).



   .. py:method:: progress_callback(*, line, client, mode, **kwargs)
      :staticmethod:


      Invoke this callback to process pod container logs.

      :param line: the read line of log.
      :param client: the Kubernetes client that can be used in the callback.
      :param mode: the current execution mode, it's one of (`sync`, `async`).
