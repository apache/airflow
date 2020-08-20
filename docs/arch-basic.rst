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



Basic airflow architecture
==========================

Primarily intended for development use, the basic airflow architecture with the Local and Sequential executors is an excellent starting point for understanding the architecture of Apache Airflow.

.. image:: img/arch-diag-basic.png


There are a few components to note:

* Metadata repository: Airflow uses a SQL database to store metadata about the data pipelines being run. In the diagram above, this is represented as Postgres which is extremely popular with Airflow. Alternate databases supported with Airflow include MySQL.

* Web Server and Scheduler: The Airflow web server and Scheduler are separate processes run (in this case) on the local machine and interact with the database mentioned above.

* The Executor is shown separately above, since it is commonly discussed within Airflow and in the documentation, but in reality it is NOT a separate process, but run within the Scheduler.

* The Worker(s) are separate processes which also interact with the other components of the Airflow architecture and the metadata repository. 

* Airflow.cfg is the Airflow configuration file which is accessed by the Web Server, Scheduler, and Workers.

* DAGs refers to the DAG files containing Python code, representing the data pipelines to be run by Airflow. The location of these files is specified in the Airflow configuration file, but they need to be accessible by the Web Server, Scheduler, and Workers.


