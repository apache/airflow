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

Alibaba Cloud Connection
========================

Authenticating to Alibaba Cloud
-------------------------------

Authentication may be performed using `Security Token Service (STS) or a signed URL <https://www.alibabacloud.com/help/doc-detail/32033.htm>`_ .

Default Connection IDs
----------------------

The default connection IDs are ``oss_default`` and ``adb_spark_default``.

Configuring the Connection
--------------------------

Schema (optional)
    Specify the default bucket name used for OSS hook.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Alibaba Cloud
    connection. The following parameters are all optional:

    * ``auth_type``: Auth type used to access Alibaba Cloud resource. Only support 'AK' now.

    * ``access_key_id``: Access key ID for Alibaba Cloud user.
    * ``access_key_secret``: Access key secret for Alibaba Cloud user.

Examples for the **Extra** field
--------------------------------

.. code-block:: json

    {
      "auth_type": "AK",
      "access_key_id": "",
      "access_key_secret": ""
    }
