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



YeeduJobRunOperator
===================
The `YeeduJobRunOperator` in this DAG facilitates the submission and monitoring of jobs using the Yeedu API in Apache Airflow. 
This enables users to execute Yeedu jobs and handle their completion status and logs seamlessly within their Airflow environment.

Use the :class:`~airflow.providers.yeedu.operators.yeedu.YeeduJobRunOperator` to submit a job to yeedu of an existing yeedu job config
via `spark/job`_ API endpoint.


Using the Operator
^^^^^^^^^^^^^^^^^^

Use the named parameters of the ``YeeduJobRunOperator``.

Required parameters:

* ``job_conf_id`` - to specify ID of the existing Yeedu job Config
* ``token`` - Yeedu API token
* ``hostname`` - Yeedu API hostname
* ``workspace_id`` - The ID of the Yeedu workspace to execute the job within
