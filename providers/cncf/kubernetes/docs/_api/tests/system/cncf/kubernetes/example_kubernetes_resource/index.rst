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

tests.system.cncf.kubernetes.example_kubernetes_resource
========================================================

.. py:module:: tests.system.cncf.kubernetes.example_kubernetes_resource

.. autoapi-nested-parse::

   This is an example DAG which uses KubernetesCreateResourceOperator and KubernetesDeleteResourceOperator.
   In this example, we create two tasks which execute sequentially.
   The first task is to create a PVC on Kubernetes cluster.
   and the second task is to delete the PVC.



Attributes
----------

.. autoapisummary::

   tests.system.cncf.kubernetes.example_kubernetes_resource.pvc_name
   tests.system.cncf.kubernetes.example_kubernetes_resource.pvc_conf
   tests.system.cncf.kubernetes.example_kubernetes_resource.ENV_ID
   tests.system.cncf.kubernetes.example_kubernetes_resource.DAG_ID
   tests.system.cncf.kubernetes.example_kubernetes_resource.t1
   tests.system.cncf.kubernetes.example_kubernetes_resource.test_run


Module Contents
---------------

.. py:data:: pvc_name
   :value: 'toto'


.. py:data:: pvc_conf
   :value: Multiline-String

   .. raw:: html

      <details><summary>Show Value</summary>

   .. code-block:: python

      """
      apiVersion: v1
      kind: PersistentVolumeClaim
      metadata:
        name: toto
      spec:
        accessModes:
          - ReadWriteOnce
        storageClassName: standard
        resources:
          requests:
            storage: 5Gi
      """

   .. raw:: html

      </details>



.. py:data:: ENV_ID

.. py:data:: DAG_ID
   :value: 'example_kubernetes_resource_operator'


.. py:data:: t1

.. py:data:: test_run
