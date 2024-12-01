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



.. _howto/operator:FTPFileTransmitOperator:

FTPFileTransmitOperator
=========================


Use the FTPFileTransmitOperator to get or
put files to/from an FTP server.

Using the Operator
^^^^^^^^^^^^^^^^^^

For parameter definition take a look at :class:`~airflow.providers.ftp.operators.FTPFileTransmitOperator`.

The below example shows how to use the FTPFileTransmitOperator to transfer a locally stored file to a remote FTP Server:

.. exampleinclude:: /../../providers/tests/system/ftp/example_ftp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ftp_put]
    :end-before: [END howto_operator_ftp_put]

The below example shows how to use the FTPFileTransmitOperator to pull a file from a remote FTP Server.

.. exampleinclude:: /../../providers/tests/system/ftp/example_ftp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ftp_get]
    :end-before: [END howto_operator_ftp_get]

.. _howto/operator:FTPSFileTransmitOperator:

FTPSFileTransmitOperator
=========================


Use the FTPSFileTransmitOperator to get or
put files to/from an FTPS server.

Using the Operator
^^^^^^^^^^^^^^^^^^

For parameter definition take a look at :class:`~airflow.providers.ftp.operators.FTPSFileTransmitOperator`.

The below example shows how to use the FTPSFileTransmitOperator to transfer a locally stored file to a remote FTPS Server:

.. exampleinclude:: /../../providers/tests/system/ftp/example_ftp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ftps_put]
    :end-before: [END howto_operator_ftps_put]

The below example shows how to use the FTPSFileTransmitOperator to pull a file from a remote FTPS Server.

.. exampleinclude:: /../../providers/tests/system/ftp/example_ftp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_ftps_get]
    :end-before: [END howto_operator_ftps_get]
