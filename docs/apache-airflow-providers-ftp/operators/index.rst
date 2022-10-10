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



.. _howto/operator:FTPOperator:

FTPOperator
===============


Use the FTPOperator to get or
pull files to/from an FTP server.

Using the Operator
^^^^^^^^^^^^^^^^^^

| **ftp_conn_id**: id referencing an FTP Connection **str**
| **local_filepath**: local file path to get or put. **str**
| **remote_filepath**: remote file path to get or put. **str**
| **operation**: specify operation 'get' or 'put' **str** - Default: **PUT**
| **create_intermediate_dirs**: create missing intermediate directories when copying from remote to local and vice-versa. **bool** - Default: **False**
|

Example: The following task would copy ``file.txt`` to the remote host
at ``/tmp/tmp1/tmp2/`` while creating ``tmp``, ``tmp1`` and ``tmp2`` if they
don't exist. If the ``create_immediate_dirs`` parameter is not passed it would error as the directory
does not exist. ::

    put_file = FTPOperator(
        task_id="test_ftp",
        ftp_conn_id="ftp_default",
        local_filepath="/tmp/file.txt",
        remote_filepath="/tmp/tmp1/tmp2/file.txt",
        operation="put",
        create_intermediate_dirs=True,
        dag=dag
    )

Another Example: The following task would copy ``file.txt`` from the remote host
at ``/remote_dir/remote_dir1/remote_dir2/`` to the local file path ``tmp/file.txt`` ::

    get_file = FTPOperator(
        task_id="test_ftp",
        ftp_conn_id="ftp_default",
        local_filepath="/tmp/file.txt",
        remote_filepath="/remote_dir/remote_dir1/remote_dir2/",
        operation="get",
        dag=dag
    )
