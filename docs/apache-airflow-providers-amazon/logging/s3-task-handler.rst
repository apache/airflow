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

.. _write-logs-amazon-s3:

Writing logs to Amazon S3
-------------------------

Remote logging to Amazon S3 uses an existing Airflow connection to read or write logs. If you
don't have a connection properly setup, this process will fail.


Enabling remote logging
'''''''''''''''''''''''

To enable this feature, ``airflow.cfg`` must be configured as follows:

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in AWS S3. Users must supply a remote
    # location URL (starting with either 's3://...') and an Airflow connection
    # id that provides access to the storage location.
    remote_logging = True
    remote_base_log_folder = s3://my-bucket/path/to/logs
    remote_log_conn_id = my_s3_conn
    # Use server-side encryption for logs stored in S3
    encrypt_s3_logs = False

In the above example, Airflow will try to use ``S3Hook(aws_conn_id='my_s3_conn')``.

You can also use `LocalStack <https://localstack.cloud/>`_ to emulate Amazon S3 locally.
To configure it, you must additionally set the endpoint url to point to your local stack.
You can do this via the Connection Extra ``host`` field.
For example, ``{"host": "http://localstack:4572"}``
