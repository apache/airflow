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



Amazon EMR Operators
====================

Amazon EMR offers several different deployment options to run Spark, Hive, and other big data workloads.

1. `Amazon EMR <https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-overview.html>`__ runs on EC2 clusters and can be used to run Steps or execute notebooks
2. `Amazon EMR on EKS <https://aws.amazon.com/emr/features/eks/>`__ runs on Amazon EKS and supports running Spark jobs
3. `Amazon EMR Serverless <https://aws.amazon.com/emr/serverless/>`__ is a serverless option that can run Spark and Hive jobs

While the EMR release can be the same across the different deployment options, you will need to configure each environment separately to support your workloads.

.. toctree::
    :maxdepth: 1
    :glob:

    *
