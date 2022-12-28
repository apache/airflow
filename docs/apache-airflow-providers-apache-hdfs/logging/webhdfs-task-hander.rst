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

.. _write-logs-apache-webhdfs:

Writing logs to Apache WebHDFS
------------------------------

Remote logging to Apache WebHDFS uses an existing Airflow connection to read or write logs. If you
don't have a connection properly setup, this process will fail.

To enable this feature, ``airflow.cfg`` must be configured as follows:

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in Apache WebHDFS. Users must supply a remote
    # location URL (starting with 'webhdfs://...') and an Airflow connection
    # id that provides write and read access to the log location.
    remote_logging = True
    remote_base_log_folder = webhdfs:///remote/log/location
    remote_log_conn_id = my_webhdfs_conn

In the above example, Airflow will try to use ``WebHDFSHook(webhdfs_conn_id='my_webhdfs_conn')``.
