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



Google Compute Engine SSH Operators
===================================

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:ComputeEngineSSHOperator:

ComputeEngineRemoteInstanceSSHOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the
:class:`~airflow.providers.ssh.operators.ssh.SSHOperator` together with
:class:`~airflow.providers.google.cloud.hooks.compute_ssh.ComputeEngineSSHHook`
to execute a command on a remote instance.

This operator uses either the Cloud OS Login or instance metadata to manage SSH keys. To use
Cloud OS Login, the service account must have ``compute.osAdminLogin`` IAM roles and the instance
metadata must have Cloud OS Login enabled. This can be done by setting the instance metadata - ``enable-oslogin=TRUE``

To use instance metadata, make sure to set the Cloud OS Login argument to False in the hook.

Please note that the target instance must allow tcp traffic on port 22.

Below is the code to create the operator:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_compute_ssh.py
    :language: python
    :dedent: 4
    :start-after: [START howto_execute_command_on_remote1]
    :end-before: [END howto_execute_command_on_remote1]

You can also create the hook without project id - project id will be retrieved
from the Google credentials used:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_compute_ssh.py
    :language: python
    :dedent: 4
    :start-after: [START howto_execute_command_on_remote2]
    :end-before: [END howto_execute_command_on_remote2]

More information
""""""""""""""""

See Google Compute Engine API documentation and Cloud OS Login API documentation
* `Google Cloud API Documentation <https://cloud.google.com/compute/docs/reference/rest/v1/>`__
* `Google Cloud OS Login API Documentation <https://cloud.google.com/compute/docs/oslogin/rest>`_.
