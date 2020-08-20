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



Distributed airflow using Kubernetes
====================================

A popular option to running Airflow on a distributed set of nodes is the Kubernetes Executor as shown below.

.. image:: img/arch-diag-kubernetes.png


In contrast to the Celery Executor, the Kubernetes Executor does not require additional components such as Redis and Flower, but does require the Kubernetes infrastructure.

The Kubernetes Executor has an advantage over the Celery Executor in that Pods are only spun up when required for task execution compared to the Celery Executor where the workers are statically configured and ran running all the time, regardless of workloads. However, this could be a disadvantage depending on the latency needs, since a task takes longer to start using the Kubernetes Executor, since it now includes the Pod startup time.

Consistent with the regular Airflow architecture, the Workers need access to the DAG files to execute the tasks within those DAGs and interact with the Metadata repository. Also, configuration information specific to the Kubernetes Executor, such as the worker namespace and image information, needs to be specified in the Airflow Configuration file.

Additionally, the Kubernetes Executor enables specification of additional features on a per-task basis using the Executor config.


