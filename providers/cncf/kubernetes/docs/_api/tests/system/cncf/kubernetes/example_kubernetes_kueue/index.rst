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

tests.system.cncf.kubernetes.example_kubernetes_kueue
=====================================================

.. py:module:: tests.system.cncf.kubernetes.example_kubernetes_kueue

.. autoapi-nested-parse::

   Example Airflow DAG for Kubernetes Kueue operators.



Attributes
----------

.. autoapisummary::

   tests.system.cncf.kubernetes.example_kubernetes_kueue.ENV_ID
   tests.system.cncf.kubernetes.example_kubernetes_kueue.DAG_ID
   tests.system.cncf.kubernetes.example_kubernetes_kueue.flavor_conf
   tests.system.cncf.kubernetes.example_kubernetes_kueue.QUEUE_NAME
   tests.system.cncf.kubernetes.example_kubernetes_kueue.local_conf
   tests.system.cncf.kubernetes.example_kubernetes_kueue.cluster_conf
   tests.system.cncf.kubernetes.example_kubernetes_kueue.install_kueue
   tests.system.cncf.kubernetes.example_kubernetes_kueue.test_run


Module Contents
---------------

.. py:data:: ENV_ID

.. py:data:: DAG_ID
   :value: 'example_kubernetes_kueue_operators'


.. py:data:: flavor_conf
   :value: Multiline-String

   .. raw:: html

      <details><summary>Show Value</summary>

   .. code-block:: python

      """
      apiVersion: kueue.x-k8s.io/v1beta1
      kind: ResourceFlavor
      metadata:
        name: default-flavor
      """

   .. raw:: html

      </details>



.. py:data:: QUEUE_NAME
   :value: 'local-queue'


.. py:data:: local_conf
   :value: Multiline-String

   .. raw:: html

      <details><summary>Show Value</summary>

   .. code-block:: python

      """
      apiVersion: kueue.x-k8s.io/v1beta1
      kind: LocalQueue
      metadata:
        namespace: default # LocalQueue under team-a namespace
        name: local-queue
      spec:
        clusterQueue: cluster-queue # Point to the ClusterQueue
      """

   .. raw:: html

      </details>



.. py:data:: cluster_conf
   :value: Multiline-String

   .. raw:: html

      <details><summary>Show Value</summary>

   .. code-block:: python

      """
      apiVersion: kueue.x-k8s.io/v1beta1
      kind: ClusterQueue
      metadata:
        name: cluster-queue
      spec:
        namespaceSelector: {}
        queueingStrategy: BestEffortFIFO
        resourceGroups:
        - coveredResources: ["cpu", "memory", "nvidia.com/gpu", "ephemeral-storage"]
          flavors:
          - name: "default-flavor"
            resources:
            - name: "cpu"
              nominalQuota: 10
            - name: "memory"
              nominalQuota: 10Gi
            - name: "nvidia.com/gpu"
              nominalQuota: 10
            - name: "ephemeral-storage"
              nominalQuota: 10Gi
      """

   .. raw:: html

      </details>



.. py:data:: install_kueue

.. py:data:: test_run
