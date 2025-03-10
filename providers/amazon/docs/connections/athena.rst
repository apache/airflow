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

.. _howto/connection:athena:

Amazon Athena Connection
==========================

The Athena connection type enables DB API 2.0 integrations with Athena.

.. note::
   This connection type is meant to be used with ``AthenaSqlHook``.
   For ``AthenaHook`` use the `Amazon Web Services Connection <./aws.html>`_ Type instead.

Authenticating to Amazon Athena
---------------------------------

Authentication may be performed using any of the authentication methods supported by `Amazon Web Services Connection <./aws.html>`_.

Default Connection IDs
----------------------

The default connection ID is ``athena_default``.

Configuring the Connection
--------------------------

Schema (optional)
  Specify the Amazon Athena database name.

Extra
    Specify the extra parameters (as json dictionary) that can be used in
    Amazon Athena connection.

    * ``region_name``: AWS Region for the connection (mandatory).
    * ``work_group``: Athena work group to use (optional).
    * ``s3_staging_dir``: Athena S3 staging directory (optional).

.. note::
   You must define either ``work_group`` or ``s3_staging_dir`` in extra field.

You can pass additional parameters to PyAthena by specifying them in the
``extra`` field of your connection as JSON. For example, to specify
please see the `documentation <https://github.com/laughingman7743/PyAthena/>`_
for PyAthena supported parameters.

Since this connection type uses authentication methods from the
`Amazon Web Services Connection <./aws.html>`_ documentation, please refer to that
for additional information about configuring the connection.
