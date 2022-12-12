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


Yandex.Cloud Data Proc Operators
================================

The `Yandex.Cloud Data Proc <https://cloud.yandex.com/services/data-proc>`__ is a service that helps to deploy Apache Hadoop®* and Apache Spark™ clusters in the Yandex.Cloud infrastructure.

You can control the cluster size, node capacity, and set of Apache® services
(Spark, HDFS, YARN, Hive, HBase, Oozie, Sqoop, Flume, Tez, Zeppelin).

Apache Hadoop is used for storing and analyzing structured and unstructured big data.

Apache Spark is a tool for quick data-processing that can be integrated with Apache Hadoop as well as with other storage systems.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^
#. Install the ``yandexcloud`` package first, like so: ``pip install 'apache-airflow[yandexcloud]'``.
#. Restart the Airflow webserver and scheduler.
#. Make sure the Yandex.Cloud connection type has been defined in Airflow. Open the connections list and look for a connection with 'yandexcloud' type.
#. Fill the required fields in Yandex.Cloud connection.

Using the operators
^^^^^^^^^^^^^^^^^^^^^
See the usage examples in `example DAGs <https://github.com/apache/airflow/tree/providers-yandex/|version|/tests/system/providers/yandex/example_yandexcloud_dataproc.py>`_
