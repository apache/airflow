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


aws_conn_id
    Reference to :ref:`Amazon Web Services Connection <howto/connection:aws>` ID.
    If this parameter is set to ``None`` then the default boto3 behaviour is used without a connection lookup.
    Otherwise use the credentials stored in the Connection. Default: ``aws_default``

region_name
    AWS Region Name. If this parameter is set to ``None`` or omitted then **region_name** from
    :ref:`AWS Connection Extra Parameter <howto/connection:aws:configuring-the-connection>` will be used.
    Otherwise use the specified value instead of the connection value. Default: ``None``

verify
    Whether or not to verify SSL certificates.

    * ``False`` - Do not validate SSL certificates.
    * **path/to/cert/bundle.pem** - A filename of the CA cert bundle to use. You can specify this argument
      if you want to use a different CA cert bundle than the one used by botocore.

    If this parameter is set to ``None`` or is omitted then **verify** from
    :ref:`AWS Connection Extra Parameter <howto/connection:aws:configuring-the-connection>` will be used.
    Otherwise use the specified value instead of the connection value. Default: ``None``

botocore_config
    The provided dictionary is used to construct a `botocore.config.Config`_.
    This configuration can be used to configure :ref:`howto/connection:aws:avoid-throttling-exceptions`, timeouts, etc.

    ..  code-block:: python
        :caption: Example, for more detail about parameters please have a look `botocore.config.Config`_

        {
            "signature_version": "unsigned",
            "s3": {
                "us_east_1_regional_endpoint": True,
            },
            "retries": {
              "mode": "standard",
              "max_attempts": 10,
            },
            "connect_timeout": 300,
            "read_timeout": 300,
            "tcp_keepalive": True,
        }

    If this parameter is set to ``None`` or omitted then **config_kwargs** from
    :ref:`AWS Connection Extra Parameter <howto/connection:aws:configuring-the-connection>` will be used.
    Otherwise use the specified value instead of the connection value. Default: ``None``

    .. note::
        Specifying an empty dictionary, ``{}``, will overwrite the connection configuration for `botocore.config.Config`_

.. _botocore.config.Config: https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
