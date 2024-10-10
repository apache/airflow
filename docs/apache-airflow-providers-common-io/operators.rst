
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

IO Operators
=============

These operators perform various operations on a filesystem or object storage.

.. _howto/operator:FileTransferOperator:

Transfer a file
~~~~~~~~~~~~~~~

Use the :class:`~airflow.providers.common.io.operators.file_transfer.FileTransferOperator` to copy a file from one
location to another. Parameters of the operator are:

- ``src`` - source path as a str or ObjectStoragePath
- ``dst`` - destination path as a str or ObjectStoragePath
- ``src_conn_id`` - source connection id (default: ``None``)
- ``dst_conn_id`` - destination connection id (default: ``None``)
- ``overwrite`` - overwrite destination (default: ``False``)

If the ``src`` and the ``dst`` are both on the same object storage, copy will be performed in the object storage.
Otherwise the data will be streamed from the source to the destination.

The example below shows how to instantiate the FileTransferOperator task.

.. exampleinclude:: /../../providers/tests/system/common/io/example_file_transfer_local_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_transfer_local_to_s3]
    :end-before: [END howto_transfer_local_to_s3]
