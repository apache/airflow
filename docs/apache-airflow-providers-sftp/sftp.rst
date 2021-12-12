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

You can use the operator for the following tasks:

1. Send one file to the server with the full path

.. code-block:: python

    put_file = SFTPOperator(
        task_id="put_file",
        ssh_conn_id="ssh_default",
        local_filepath="/tmp/transfer_file/put_file_file1.txt",
        remote_filepath="/tmp/transfer_file/remote/put_file_file1.txt",
        operation=SFTPOperation.PUT,
        create_intermediate_dirs=True,
    )


2. Send all files from local directory to remote server

.. code-block:: python

    put_dir_files = SFTPBatchOperator(
        task_id="put_dir_files",
        ssh_conn_id="ssh_default",
        local_folder="/tmp/local_folder/",
        remote_folder="/tmp/dir_for_remote_transfer/",
        operation=SFTPOperation.PUT,
        create_intermediate_dirs=True,
    )


3. Send specific files from local directory to remote server

.. code-block:: python

    put_dir_files = SFTPBatchOperator(
        task_id="put_dir_files",
        ssh_conn_id="ssh_default",
        local_files_path=[
            "/tmp/local_folder/file1.txt",
        ],
        remote_folder="/tmp/dir_for_remote_transfer/",
        operation=SFTPOperation.PUT,
        create_intermediate_dirs=True,
    )


4. Send all files from the local directory that match the specified pattern to the remote server

.. code-block:: python

    put_dir_txt_files = SFTPBatchOperator(
        task_id="put_dir_txt_files",
        ssh_conn_id="ssh_default",
        local_folder="/tmp/local_folder/",
        remote_folder="/tmp/dir_for_remote_transfer/",
        regexp_mask=r".*\.txt",
        operation=SFTPOperation.PUT,
        create_intermediate_dirs=True,
    )


5. Get specific list of files from the remote server to the local folder

.. code-block:: python

    put_dir_txt_files = SFTPBatchOperator(
        task_id="put_dir_txt_files",
        ssh_conn_id="ssh_default",
        local_folder="/tmp/local_folder/",
        remote_files_path=[
            "/tmp/dir_for_remote_transfer/file1.txt",
            "/tmp/dir_for_remote_transfer/file2.txt",
        ],
        operation=SFTPOperation.GET,
        create_intermediate_dirs=True,
    )


6. Get all files from the remote server to the local folder

.. code-block:: python

    put_dir_txt_files = SFTPBatchOperator(
        task_id="put_dir_txt_files",
        ssh_conn_id="ssh_default",
        local_folder="/tmp/local_folder/",
        remote_folder="/tmp/dir_for_remote_transfer/",
        operation=SFTPOperation.GET,
        create_intermediate_dirs=True,
    )


7. Get all files from the remote server that match the specified pattern to the local folder with overwrite files

.. code-block:: python

    put_dir_txt_files = SFTPBatchOperator(
        task_id="put_dir_txt_files",
        ssh_conn_id="ssh_default",
        local_folder="/tmp/local_folder/",
        remote_folder="/tmp/dir_for_remote_transfer/",
        regexp_mask=r".*\.txt",
        operation=SFTPOperation.GET,
        create_intermediate_dirs=True,
        force=True,
    )



Parameter ``create_intermediate_dirs`` is needed to create missing intermediate directories when
copying from remote to local and vice-versa. Default is False.

Parameter ``force`` is needed to overwrite file if it already exist. Default is False.
