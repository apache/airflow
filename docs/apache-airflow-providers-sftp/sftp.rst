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

SFTPOperator
==========================
Use the :class:`~airflow.providers.sftp.operators.sftp.py` to
transfer data between servers under sftp.

Using the Operator
------------------
To start working with an operator, you need to register an SFTP \ SSH connection in Airflow Connections.
Use ssh_conn_id to specify the name of the connection.

You can use SFTPOperator for send one file to remote server

.. code-block:: python

    put_file = SFTPOperator(
        task_id="put_file",
        ssh_conn_id="ssh_default",
        local_filepath="/tmp/transfer_file/put_file_file1.txt",
        remote_filepath="/tmp/transfer_file/remote/put_file_file1.txt",
        operation=SFTPOperation.PUT,
        create_intermediate_dirs=True,
    )


And you can use SFTPOperator for get one file from remote server to local

.. code-block:: python

    put_file = SFTPOperator(
        task_id="get_file",
        ssh_conn_id="ssh_default",
        local_filepath="/tmp/transfer_file/remote/put_file_file1.txt",
        remote_filepath="/tmp/transfer_file/put_file_file1.txt",
        operation=SFTPOperation.GET,
        create_intermediate_dirs=True,
    )



Parameter ``operation`` needs for position determination:
    1.  ``SFTPOperation.GET`` or ``"GET"`` for get file from remote to local
    2.  ``SFTPOperation.PUT`` or ``"PUT"`` for get file from local to remote

Parameter ``create_intermediate_dirs`` is needed to create missing intermediate directories when
copying from remote to local and vice-versa. Default is False.
