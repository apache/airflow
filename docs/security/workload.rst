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

Workload
========

This topic describes how to configure Airflow to secure your workload.

.. contents::
  :depth: 1
  :local:

Impersonation
-------------

Airflow has the ability to impersonate a unix user while running task
instances based on the task's ``run_as_user`` parameter, which takes a user's name.

**NOTE:** For impersonations to work, Airflow must be run with ``sudo`` as subtasks are run
with ``sudo -u`` and permissions of files are changed. Furthermore, the unix user needs to
exist on the worker. Here is what a simple sudoers file entry could look like to achieve
this, assuming as airflow is running as the ``airflow`` user. Note that this means that
the airflow user must be trusted and treated the same way as the root user.

.. code-block:: none

    airflow ALL=(ALL) NOPASSWD: ALL


Subtasks with impersonation will still log to the same folder, except that the files they
log to will have permissions changed such that only the unix user can write to it.

Default Impersonation
'''''''''''''''''''''
To prevent tasks that don't use impersonation to be run with ``sudo`` privileges, you can set the
``core:default_impersonation`` config which sets a default user impersonate if ``run_as_user`` is
not set.

.. code-block:: ini

    [core]
    default_impersonation = airflow
