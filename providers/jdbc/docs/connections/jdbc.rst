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

.. _howto/connection:jdbc:

JDBC connection
===============

The JDBC connection type enables connection to a JDBC data source.

Configuring the Connection
--------------------------

Host (required)
    The host to connect to.

Schema (required)
    Specify the database name to be used in.

Login (required)
    Specify the user name to connect to.

Password (required)
    Specify the password to connect to.

Port (optional)
    Port of host to connect to. Not user in ``JdbcOperator``.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in JDBC connection. The following parameters out of the standard python parameters are supported:

    - ``driver_class``
        * Full qualified Java class name of the JDBC driver. For ``JdbcOperator``.
          Note that this is only considered if ``allow_driver_class_in_extra`` is set to True in airflow config section
          ``providers.jdbc`` (by default it is not considered).  Note: if setting this config from env vars, use
          ``AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_CLASS_IN_EXTRA=true``.

    - ``driver_path``
        * Jar filename or sequence of filenames for the JDBC driver libs. For ``JdbcOperator``.
          Note that this is only considered if ``allow_driver_path_in_extra`` is set to True in airflow config section
          ``providers.jdbc`` (by default it is not considered).  Note: if setting this config from env vars, use
          ``AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_PATH_IN_EXTRA=true``.

    .. note::
        Setting ``allow_driver_path_in_extra`` or ``allow_driver_class_in_extra`` to True allows users to set the driver
        via the Airflow Connection's ``extra`` field.  By default this is not allowed.  If enabling this functionality,
        you should make sure that you trust the users who can edit connections in the UI to not use it maliciously.
