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

.. _write-logs-hdfs:

Writing logs to HDFS
---------------------------

Remote logging to HDFS uses an existing Airflow connection to read or write logs. If you
don't have a connection properly setup, this process will fail.


Enabling remote logging
'''''''''''''''''''''''

To enable this feature, ``airflow.cfg`` must be configured as follows:

.. code-block:: ini

    [logging]
    # Airflow can store logs remotely in HDFS. Users must supply a remote
    # location URL (starting with either 'hdfs://...') and an Airflow connection
    # id that provides access to the storage location.
    remote_logging = True
    remote_base_log_folder = hdfs://some/path/to/logs
    remote_log_conn_id = webhdfs_default

In the above example, Airflow will try to use ``WebHDFSHook('webhdfs_default')``.
