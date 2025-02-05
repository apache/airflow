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

airflow.providers.cncf.kubernetes.decorators.kubernetes
=======================================================

.. py:module:: airflow.providers.cncf.kubernetes.decorators.kubernetes


Functions
---------

.. autoapisummary::

   airflow.providers.cncf.kubernetes.decorators.kubernetes.kubernetes_task


Module Contents
---------------

.. py:function:: kubernetes_task(python_callable = None, multiple_outputs = None, **kwargs)

   Kubernetes operator decorator.

   This wraps a function to be executed in K8s using KubernetesPodOperator.
   Also accepts any argument that DockerOperator will via ``kwargs``. Can be
   reused in a single DAG.

   :param python_callable: Function to decorate
   :param multiple_outputs: if set, function return value will be
       unrolled to multiple XCom values. Dict will unroll to xcom values with
       keys as XCom keys. Defaults to False.
