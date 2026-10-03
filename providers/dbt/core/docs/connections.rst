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

.. _howto/connection:dbt_core:

Connections used by DbtKubernetesRunOperator
============================================

:class:`~airflow.providers.dbt.core.operators.dbt.DbtKubernetesRunOperator`
does not define a connection type of its own. Instead it reads up to three
standard Airflow connections **on the worker** and injects the resolved values
into the pod as environment variables. All three are optional — supply only the
ones your job needs.

Git connection (``git_conn_id``)
--------------------------------

Used only when ``git_repo_url`` is set and the repository is private. The
connection's **password** field holds the git access token (for example a
personal access token or deploy token). The operator reads it on the worker and
injects it as ``GIT_TOKEN`` for the clone step only; the clone URL becomes
``https://<token>@<git_repo_url>``.

Only the password is used — host, login, schema and extras are ignored. If the
repository is public, or the project is baked into the image, omit
``git_conn_id`` (and ``git_repo_url``). See :doc:`/security` for a note on how
the token appears transiently in the clone URL.

Warehouse connection (``warehouse_conn_id``)
--------------------------------------------

Provides the credentials your ``profiles.yml`` needs to reach the data
warehouse. The connection fields are mapped to environment variables in the
pod:

.. list-table::
    :header-rows: 1
    :widths: 40 60

    * - Connection field
      - Environment variable
    * - Host
      - ``DBT_HOST``
    * - Login
      - ``DBT_USER``
    * - Password
      - ``DBT_PASSWORD``
    * - Schema
      - ``DBT_SCHEMA``
    * - Port
      - ``DBT_PORT``

Empty fields are skipped. Reference these variables from ``profiles.yml`` with
dbt's ``env_var()`` function, for example:

.. code-block:: yaml

    my_project:
      target: prod
      outputs:
        prod:
          type: postgres
          host: "{{ env_var('DBT_HOST') }}"
          user: "{{ env_var('DBT_USER') }}"
          password: "{{ env_var('DBT_PASSWORD') }}"
          schema: "{{ env_var('DBT_SCHEMA') }}"
          port: "{{ env_var('DBT_PORT') | int }}"

Use whatever connection type matches your warehouse (Postgres, Snowflake, ...);
only the standard host / login / password / schema / port fields are read.

Artifact connection (``artifact_conn_id``)
------------------------------------------

Resolves the credentials used to upload ``target/`` to object storage. The
**scheme of** ``artifact_dest`` selects the connection type and the variables
injected into the pod:

.. list-table::
    :header-rows: 1
    :widths: 20 35 45

    * - ``artifact_dest`` scheme
      - Connection type
      - Injected into the pod
    * - ``s3://``
      - Amazon Web Services
      - ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY`` (plus
        ``AWS_SESSION_TOKEN`` for temporary credentials)
    * - ``gs://``
      - Google Cloud
      - ``GOOGLE_APPLICATION_CREDENTIALS_JSON``

For **S3**, credentials are read via
:class:`~airflow.providers.amazon.aws.hooks.s3.S3Hook` and injected as the
standard AWS environment variables, so ``aws s3 sync`` in the pod authenticates
transparently. Requires ``apache-airflow-providers-amazon``.

For **GCS**, the connection must define a service-account ``keyfile_dict`` (the
key JSON inline, not a path). The operator injects it as
``GOOGLE_APPLICATION_CREDENTIALS_JSON``; the in-pod script writes it to a file
and points ``GOOGLE_APPLICATION_CREDENTIALS`` at it for ``gsutil``. A connection
that only references a keyfile *path* (or relies on Application Default
Credentials on the worker) will not work, because the key material must travel
into the pod. Requires ``apache-airflow-providers-google``.

Because these credentials become environment variables in the pod spec, read
:doc:`/security` for the tradeoff and hardening options.
