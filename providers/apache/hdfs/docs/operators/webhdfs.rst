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



WebHDFS Operators
=================

WebHDFS provides web services access to data stored in HDFS. At the same time,
it retains the security the native Hadoop protocol offers and uses parallelism, for better throughput.

WebHdfsSensor
-------------

Waits for a file or folder to land in HDFS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hdfs.sensors.web_hdfs.WebHdfsSensor` is used to check for a file or folder
to land in HDFS.

Use the ``filepath`` parameter to poke until the provided file is found.

Reference
^^^^^^^^^

For further information, look at `WebHDFS REST API  <https://hadoop.apache.org/docs/r1.0.4/webhdfs.html>`_.
