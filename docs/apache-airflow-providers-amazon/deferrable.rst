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

========================
AWS Deferrable Operators
========================

The AWS deferrable operators depends on ``aiobotocore>=2.1.1`` library. Unfortunately, currently we can't add this to
core AWS provider dependencies because of a conflicting version of ``botocore`` between ``aiobotocore`` and ``boto3``.
We have added ``aiobotocore`` as an addition dependency. So if you want to use AWS deferrable operator then you will have to
manage this by yourself.

We have introduced an async hook to manage authentication between to the AWS services asynchronously. The
AWS async hook currently support the default botocore authentication mechanism i.e if Airflow connection is
not provided then provider will try to find the credential param in environment variable. If the Airflow connection is
provided then basic auth with secret-key/access-key-id/profile/token and arn-method should work.

To use deferrable operator we have exposed ``deferrable`` param in those operators which support deferrable execution.
By default ``deferrable`` is set to ``False`` set this to ``True`` to run operator in asynchronous mode.
