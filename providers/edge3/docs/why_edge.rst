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

Why using Edge Worker?
======================

Apache Airflow implements a distributed execution architecture. The Airflow scheduler
is responsible for scheduling tasks and sending them to the workers. The workers are
responsible for executing the tasks. The Airflow scheduler and workers are typically
deployed in the same data center.

Most popular execution options for distributed setups are based on the CeleryExecutor or
KubernetesExecutor. The CeleryExecutor is a distributed task queue that allows you to run
tasks in parallel across multiple workers. These workers are connected via a task queue,
typically using Redis or RabbitMQ.
The KubernetesExecutor is a cloud-native execution option that allows you to run tasks in
Kubernetes Pods. The KubernetesExecutor is a great option for organizations that are already
using Kubernetes for their infrastructure. It allows you to take advantage of the scalability
and flexibility of Kubernetes to run your Airflow tasks. However, it requires a Kubernetes
cluster and a Kubernetes service account with the necessary permissions to create and manage
Pods.

The Edge Worker is a execution option that allows you to run Airflow tasks on edge devices.
The Edge Worker is designed to be lightweight and easy to deploy. It allows you to run Airflow
tasks on machines that are not part of your main data center, e.g. edge servers. This is
especially useful when deployments need to cross multiple data centers or security perimeters
like firewalls. For Celery for example a stable TCP connection is required between the task
queue (e.g. Redis) and the workers which can be hard to operate on wide-area networks.
To run Kubernetes Pods the scheduler needs access to API endpoints of the Kubernetes cluster
which is not always possible in edge deployments. Alternatively sometimes it is possible to
execute work on the edge devices via SSHOperator but this requires also a direct and stable
TCP connection to the edge devices.

Target of the Edge Worker is to have a lean setup that allows task execution on edge devices
with only (outbound) HTTPS access. Edge Workers will be able connect, pull and execute tasks
with a simple deployment.

.. image:: img/distributed_architecture.svg
   :alt: Distributed architecture
