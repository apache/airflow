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



.. _howto/connection:postgres:

PostgreSQL Connection
======================
The Postgres connection type provides connection to a Postgres database.

Configuring the Connection
--------------------------
Host (required)
    The host to connect to.

Database (optional)
    Specify the name of the database to connect to.

    .. note::

        If you want to define a default database schema:

        * using :class:`~airflow.providers.postgres.operators.postgres.PostgresOperator`
          see :ref:`Passing Server Configuration Parameters into PostgresOperator <howto/operators:postgres>`
        * using :class:`~airflow.providers.postgres.hooks.postgres.PostgresHook`
          see `search_path <https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH>`_

Login (required)
    Specify the user name to connect.

Password (required)
    Specify the password to connect.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in Postgres
    connection. The following parameters out of the standard python parameters
    are supported:

    * ``sslmode`` - This option determines whether or with what priority a secure SSL
      TCP/IP connection will be negotiated with the server. There are six modes:
      ``disable``, ``allow``, ``prefer``, ``require``, ``verify-ca``, ``verify-full``.
    * ``sslcert`` - This parameter specifies the file name of the client SSL certificate,
      replacing the default.
    * ``sslkey`` - This parameter specifies the file name of the client SSL key,
      replacing the default.
    * ``sslrootcert`` - This parameter specifies the name of a file containing SSL
      certificate authority (CA) certificate(s).
    * ``sslcrl`` - This parameter specifies the file name of the SSL certificate
      revocation list (CRL).
    * ``application_name`` - Specifies a value for the application_name
      configuration parameter.
    * ``keepalives_idle`` - Controls the number of seconds of inactivity after which TCP
      should send a keepalive message to the server.
    * ``client_encoding``: specifies client encoding(character set) of the client connection.
      Refer to `Postgres supported character sets <https://www.postgresql.org/docs/current/multibyte.html>`_
    * ``cursor`` - Specifies the cursor type to use when querying the database. You can choose one of the following:

      - ``dictcursor``: Returns query results as Python dictionaries using ``psycopg2.extras.DictCursor``.
      - ``realdictcursor``: Similar to ``DictCursor``, but uses ``psycopg2.extras.RealDictCursor`` for slightly better performance.
      - ``namedtuplecursor``: Returns query results as named tuples using ``psycopg2.extras.NamedTupleCursor``.

      For more information, refer to the psycopg2 documentation on `connection and cursor subclasses <https://www.psycopg.org/docs/extras.html#connection-and-cursor-subclasses>`_.

    More details on all Postgres parameters supported can be found in
    `Postgres documentation <https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING>`_.

    Example "extras" field:

    .. code-block:: json

       {
          "sslmode": "verify-ca",
          "sslcert": "/tmp/client-cert.pem",
          "sslca": "/tmp/server-ca.pem",
          "sslkey": "/tmp/client-key.pem"
       }

    The following extra parameters use for additional Hook configuration:

    * ``iam`` - If set to ``True`` than use AWS IAM database authentication for
      `Amazon RDS <https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html>`__,
      `Amazon Aurora <https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/UsingWithRDS.IAMDBAuth.html>`__
      or `Amazon Redshift <https://docs.aws.amazon.com/redshift/latest/mgmt/generating-user-credentials.html>`__.
    * ``aws_conn_id`` - AWS Connection ID which use for authentication via AWS IAM,
      if not specified then **aws_default** is used.
    * ``redshift`` - Used when AWS IAM database authentication enabled.
      If set to ``True`` than authenticate to Amazon Redshift Cluster, otherwise to Amazon RDS or Amazon Aurora.
    * ``cluster-identifier`` - The unique identifier of the Amazon Redshift Cluster that contains the database
      for which you are requesting credentials. This parameter is case sensitive.
      If not specified than hostname from **Connection Host** is used.

    Example "extras" field (Amazon RDS PostgreSQL or Amazon Aurora PostgreSQL):

    .. code-block:: json

       {
          "iam": true,
          "aws_conn_id": "aws_awesome_rds_conn"
       }

    Example "extras" field (Amazon Redshift):

    .. code-block:: json

       {
          "iam": true,
          "aws_conn_id": "aws_awesome_redshift_conn",
          "redshift": "/tmp/server-ca.pem",
          "cluster-identifier": "awesome-redshift-identifier"
       }

    When specifying the connection as URI (in :envvar:`AIRFLOW_CONN_{CONN_ID}` variable) you should specify it
    following the standard syntax of DB connections, where extras are passed as parameters
    of the URI (note that all components of the URI should be URL-encoded).

    For example:

    .. code-block:: bash

        export AIRFLOW_CONN_POSTGRES_DEFAULT='postgresql://postgres_user:XXXXXXXXXXXX@1.1.1.1:5432/postgresdb?sslmode=verify-ca&sslcert=%2Ftmp%2Fclient-cert.pem&sslkey=%2Ftmp%2Fclient-key.pem&sslrootcert=%2Ftmp%2Fserver-ca.pem'
