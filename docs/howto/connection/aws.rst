..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Amazon Web Services Connection
==============================

The Amazon Web Services connection type enables the :ref:`AWS Integrations
<AWS>`.

Authenticating to AWS
---------------------

Authentication may be performed using any of the `boto3 options <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#configuring-credentials>`_. Alternatively, one can pass credentials in as a Connection initialisation parameter.

To use IAM instance profile, create an "empty" connection (i.e. one with no Login or Password specified).

Default Connection IDs
-----------------------

The default connection ID is ``aws_default``.

Configuring the Connection
--------------------------


Login (optional)
    Specify the AWS access key ID.

Password (optional)
    Specify the AWS secret access key.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in AWS
    connection. The following parameters are supported:

    * **aws_account_id**: AWS account ID for the connection
    * **aws_iam_role**: AWS IAM role for the connection
    * **external_id**: AWS external ID for the connection
    * **host**: Endpoint URL for the connection
    * **region_name**: AWS region for the connection
    * **role_arn**: AWS role ARN for the connection

    Example "extras" field:

    .. code-block:: json

       {
          "aws_iam_role": "aws_iam_role_name",
          "region_name": "ap-southeast-2"
       }
