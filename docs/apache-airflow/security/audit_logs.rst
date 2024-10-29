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

Audit Logs in Airflow
=====================


Overview
---------

Audit logs are a critical component of any system that needs to maintain a high level of security and compliance.
They provide a way to track user actions and system events, which can be used to troubleshoot issues, detect security breaches, and ensure regulatory compliance.

In Airflow, audit logs are used to track user actions and system events that occur during the execution of Dags and tasks.
They are stored in a database and can be accessed through the Airflow UI.

To be able to see audit logs, a user needs to have the ``Audit Logs.can_read`` permission. Such user will be able to see all audit logs, independently of the Dags permissions applied.


Level of Audit Logs
--------------------

Audit logs exist at the task level and the user level.

- Task Level: At the task level, audit logs capture information related to the execution of a task, such as the start time, end time, and status of the task.

- User Level: At the user level, audit logs capture information related to user actions, such as creating, modifying, or deleting a DAG or task.


Location of Audit Logs
----------------------

Audit logs can be accessed through the Airflow UI. They are located under the "Browse" tab, and can be viewed by selecting "Audit Logs" from the dropdown menu.


Types of Events
---------------

Airflow provides a set of predefined events that can be tracked in audit logs. These events include, but aren't limited to:

- ``trigger``: Triggering a DAG
- ``[variable,connection].create``: A user created a Connection or Variable
- ``[variable,connection].edit``: A user modified a Connection or Variable
- ``[variable,connection].delete``: A user deleted a Connection or Variable
- ``delete``: A user deleted a DAG or task
- ``failed``: Airflow or a user set a task as failed
- ``success``: Airflow or a user set a task as success
- ``retry``: Airflow or a user retried a task instance
- ``clear``: A user cleared a task's state
- ``cli_task_run``: Airflow triggered a task instance

In addition to these predefined events, Airflow allows you to define custom events that can be tracked in audit logs.
This can be done by calling the ``log`` method of the ``TaskInstance`` object.
