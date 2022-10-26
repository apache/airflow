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

Google Cloud Platform SSH Connection
====================================

The SSH connection type provides connection to Compute Engine Instance.
The :class:`~airflow.providers.google.cloud.hooks.compute_ssh.ComputeEngineSSHHook` use it to run
commands on a remote server using :class:`~airflow.providers.ssh.operators.ssh.SSHOperator` or transfer
file from/to the remote server using :class:`~airflow.providers.sftp.operators.sftp.SFTPOperator`.


Configuring the Connection
--------------------------

For authorization to Google Cloud services, this connection should contain a configuration identical to the :doc:`/connections/gcp`.
All parameters for a Google Cloud connection are also valid configuration parameters for this connection.

In addition, additional connection parameters to the instance are supported. It is also possible to pass them
as the parameter of hook constructor, but the connection configuration takes precedence over the parameters
of the hook constructor.

Host (required)
    The Remote host to connect. If it is not passed, it will be detected
    automatically.

Username (optional)
    The Username to connect to the ``remote_host``.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in ssh
    connection. The following parameters are supported in addition to those describing
    the Google Cloud connection.

    * ``instance_name`` - The name of the Compute Engine instance.
    * ``zone`` - The zone of the Compute Engine instance.
    * ``use_internal_ip`` - Whether to connect using internal IP.
    * ``use_iap_tunnel`` - Whether to connect through IAP tunnel.
    * ``use_oslogin`` - Whether to manage keys using OsLogin API. If false, keys are managed using instance metadata.
    * ``expire_time`` - The maximum amount of time in seconds before the private key expires.


Environment variable
--------------------

You can also create a connection using an :envvar:`AIRFLOW_CONN_{CONN_ID}` environment variable.

For example:

.. code-block:: bash

    export AIRFLOW_CONN_GOOGLE_CLOUD_SQL_DEFAULT="gcpssh://conn-user@conn-host?\
    instance_name=conn-instance-name&\
    zone=zone&\
    use_internal_ip=True&\
    use_iap_tunnel=True&\
    use_oslogin=False&\
    expire_time=4242"
