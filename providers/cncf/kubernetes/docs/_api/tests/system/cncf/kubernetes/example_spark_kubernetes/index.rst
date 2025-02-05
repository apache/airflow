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

tests.system.cncf.kubernetes.example_spark_kubernetes
=====================================================

.. py:module:: tests.system.cncf.kubernetes.example_spark_kubernetes

.. autoapi-nested-parse::

   This is an example DAG which uses SparkKubernetesOperator and SparkKubernetesSensor.
   In this example, we create two tasks which execute sequentially.
   The first task is to submit sparkApplication on Kubernetes cluster(the example uses spark-pi application).
   and the second task is to check the final state of the sparkApplication that submitted in the first state.

   Spark-on-k8s operator is required to be already installed on Kubernetes
   https://github.com/GoogleCloudPlatform/spark-on-k8s-operator



Attributes
----------

.. autoapisummary::

   tests.system.cncf.kubernetes.example_spark_kubernetes.ENV_ID
   tests.system.cncf.kubernetes.example_spark_kubernetes.DAG_ID
   tests.system.cncf.kubernetes.example_spark_kubernetes.pi_example_path
   tests.system.cncf.kubernetes.example_spark_kubernetes.test_run


Module Contents
---------------

.. py:data:: ENV_ID

.. py:data:: DAG_ID
   :value: 'spark_pi'


.. py:data:: pi_example_path

.. py:data:: test_run
