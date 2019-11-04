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

Getting a Production Ready DAG
==============================


Running Airflow in production is seamless. It comes bundled with all the plugins and configs
necessary to run most of the DAGs. However, you can come across certain pitfalls which can cause irregular errors.
Let's the steps you need to follow to avoid these pitfalls.

Writing a DAG
^^^^^^^^^^^^^^
Creating a new DAG in Airflow is quite simple. However, their are a lot of things which you need to take care of
to ensure the DAG run or failure do not produce unexpected results.

Creating a task
------------------

You should treat tasks in Airflow equivalent to transactions in database. It implies that you should never produce
incomplete results from your tasks. An example will be not to produce incomplete data in HDFS/S3 at the end of task.

Airflow retries a task if it fails. Thus, the tasks should produce the same outcome on every re-run.
Some of the ways you can avoid producing different result -
    * Don't use INSERT - During a task re-run an INSERT statement might lead to duplicate rows in your database.
        Replace it with UPSERT.
    * Read and write in a specific partition - Never read the latest available data in a task. It can happen that the input data is 
    updated between re-runs which results in different output. A better way to do it is to read the input data from a specific partition
    such as the ``execution_date`` of the DAG. This partition method should be followed while writing data in S3/HDFS as well.
    * Don't use now() - The python datetime now() function gives the current datetime object. This should never be used inside a task, especially
        to do critical computation, as it leads to different outcomes on each run. It's fine to use it, for example, to generate a temporary log.


Deleting a task
----------------

Never delete a task from a DAG. In case of deletion, the historical information of the task disappers from the Airflow UI. It is advised to create a new DAG
in case the tasks need to be deleted.


Communication
--------------

Airflow tasks should also not store any file or config in the local filesystem. For example, a task which downloads the JAR file to be executed in next step.
This is because all tasks are executed in different directories which can even be present on different servers in case you are using Kubernetes executor or Celery executor.
Always use XCom to communicate small messages between tasks or S3/HDFS to communicate large messages/files.

The tasks should also not store any authentication parameters such as passwords or token inside them. Always use ``Connections`` to store data securely in Airflow backend
and retrieve them using unique connection id.

Additional Precautions
----------------------

Don't write any critical code outside the tasks. The code outside the tasks runs every time airflow parses the DAG which happens too frequently (TODO: insert interval here).

You should also avoid repeating arguments such as connection_id or S3 paths using default_args. It helps you to avoid mistakes while passing arguments.



Testing a DAG
^^^^^^^^^^^^^
