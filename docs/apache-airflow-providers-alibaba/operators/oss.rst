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

Alibaba Cloud OSS Operators
===========================

Overview
--------

Airflow to Alibaba Cloud Object Storage Service (OSS) integration provides several operators to create and interact with OSS buckets.

 - :class:`~airflow.providers.alibaba.cloud.sensors.oss_key.OSSKeySensor`
 - :class:`~airflow.providers.alibaba.cloud.operators.oss.OSSCreateBucketOperator`
 - :class:`~airflow.providers.alibaba.cloud.operators.oss.OSSDeleteBucketOperator`
 - :class:`~airflow.providers.alibaba.cloud.operators.oss.OSSUploadObjectOperator`
 - :class:`~airflow.providers.alibaba.cloud.operators.oss.OSSDownloadObjectOperator`
 - :class:`~airflow.providers.alibaba.cloud.operators.oss.OSSDeleteBatchObjectOperator`
 - :class:`~airflow.providers.alibaba.cloud.operators.oss.OSSDeleteObjectOperator`

Create and Delete Alibaba Cloud OSS Buckets
-------------------------------------------

Purpose
"""""""

This example dag uses ``OSSCreateBucketOperator`` and ``OSSDeleteBucketOperator`` to create a
new OSS bucket with a given bucket name then delete it.

Defining tasks
""""""""""""""

In the following code we create a new bucket and then delete the bucket.

.. exampleinclude:: /../../providers/tests/system/alibaba/example_oss_bucket.py
    :language: python
    :start-after: [START howto_operator_oss_bucket]
    :end-before: [END howto_operator_oss_bucket]
