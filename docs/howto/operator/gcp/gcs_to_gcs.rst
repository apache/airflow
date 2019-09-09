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

Transfer data in Google Cloud Storage
-------------------------------------

This page will show operators whose purpose is to copy data as part of the Google Cloud Service.
Each of them has its own specific use cases, as well as limitations.

Cloud Storage Transfer Service
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are many operators that manage the Google Cloud Data Transfer service. If you want to create a new data transfer
task, use the operator  :class:`~airflow.gcp.operators.cloud_storage_transfer_service.GcpTransferServiceJobCreateOperator`
You can also use the previous operator for this service -
:class:`~airflow.gcp.operators.cloud_storage_transfer_service.GoogleCloudStorageToGoogleCloudStorageTransferOperator`

These operators does not control the copying process locally, but uses Google resources, which allows to
perform this task faster and more economically. The economic effects are especially prominent when the
existence of Airflow is not found in the Google Cloud Platform, because this operator allows egress
traffic reductions.

This operator modifies source objects if the option that specifies whether objects should be deleted
from the source after they are transferred to the sink is enabled.

When you use this service, you can specify whether overwriting objects that already exist in the sink is
allowed, whether objects that exist only in the sink should be deleted, or whether objects should be deleted
from the source after they are transferred to the sink.

Source objects can be specified using include and exclusion prefixes, as well as based on the file
modification date.

If you need information on how to use it, look at the guide: :doc:`transfer`

Local transfer
~~~~~~~~~~~~~~

There are two operators that are used to copy data, where the entire process is controlled locally.

GoogleCloudStorageToGoogleCloudStorageOperator
----------------------------------------------

:class:`~airflow.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator` allows you to copy
one or more files. The copying always takes place without taking into account the initial state of
the destination bucket.

This operator never deletes data in the destination bucket and it deletes objects in the source bucket
if the file move option is active.

When you use this operator, you can specify whether objects should be deleted from the source after
they are transferred to the sink. Source objects can be specified using single wildcard, as
well as based on the file modification date.

The way this operator works can be compared to the ``cp`` command.

GoogleCloudStorageSynchronizeBuckets
------------------------------------

The :class:`~airflow.operators.gcs_to_gcs.GoogleCloudStorageSynchronizeBuckets`
operator checks the initial state of the destination bucket, and then compares it with the source bucket.
Based on this, it creates an operation plan that describes which objects should be deleted from
the destination bucket, which should be overwritten, and which should be copied.

This operator never modifies data in the source bucket.

When you use this operator, you can specify whether
overwriting objects that already exist in the sink is allowed, whether
objects that exist only in the sink should be deleted, whether subdirectories are to be processed or
which subdirectory is to be processed.

The way this operator works can be compared to the ``rsync`` command.
