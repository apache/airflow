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



Kubernetes Executor
===================

The kubernetes executor is introduced in Apache Airflow 1.10.0. The Kubernetes executor will create a new pod for every task instance.

Example helm charts are available at ``scripts/ci/kubernetes/kube/{airflow,volumes,postgres}.yaml`` in the source distribution.
The volumes are optional and depend on your configuration. There are two volumes available:

- **Dags**:

  - By storing dags onto persistent disk, it will be made available to all workers

  - Another option is to use ``git-sync``. Before starting the container, a git pull of the dags repository will be performed and used throughout the lifecycle of the pod

- **Logs**:

  - By storing logs onto a persistent disk, the files are accessible by workers and the webserver. If you don't configure this, the logs will be lost after the worker pods shuts down

  - Another option is to use S3/GCS/etc to store logs
