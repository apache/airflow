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


Amazon SFTP to S3 Transfer Operator
===================================

AWS Transfer for SFTP provides Secure File Transfer Protocol (SFTP) access to a customer's S3 resources. For more information about the service visit `Amazon Transfer for SFTP API documentation <https://docs.aws.amazon.com/whitepapers/latest/architecting-hipaa-security-and-compliance-on-aws/aws-transfer-for-sftp.html>`_

.. _howto/operator:SFTPToS3Operator:

SFTPToS3Operator
^^^^^^^^^^^^^^^^

This operator enables the transferring of files from a SFTP server to Amazon S3.

To get more information about operator visit:
:class:`~airflow.providers.amazon.aws.transfers.sftp_to_s3.SFTPToS3Operator`

Example usage:

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_sftp_to_s3.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sftp_transfer_data_to_s3]
    :end-before: [END howto_sftp_transfer_data_to_s3]
