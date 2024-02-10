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



OpenSearch Connection
=====================
The OpenSearch connection provides credentials for an OpenSearch instance.

Configuring the Connection
--------------------------
Host (required)
  The host address of the OpenSearch instance.
Login (required)
  The login user.
Password (required)
  The password for the login user.
Extra (optional)
  Specifying the extra parameters as a (json dictionary) that can be used in the OpenSearch connection.
  The following parameters are all optional:

  * ``use_ssl``: Boolean on requiring an ssl connection. Default is false.
  * ``verify_certs``: Boolean indicating to verify certs for ssl. Default is false.
