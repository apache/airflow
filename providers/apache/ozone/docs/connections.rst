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


.. _howto/connection:ozone:

Apache Ozone Connections
========================

The ``ozone`` connection type is the main runtime contract for this provider.
It is used by hooks/operators/sensors/transfers for Native Ozone CLI execution
(``ozone sh`` / ``ozone fs``) and for security/runtime settings.

Provider runtime reads this connection via:
``airflow.providers.apache.ozone.utils.connection_schema``
(``OzoneConnSnapshot.from_connection(...)``).

The worker runtime must have:

* Java runtime
* Ozone CLI binary available in ``PATH``
* client config directory/files reachable from the worker runtime

Default connection IDs
----------------------

* ``ozone_default`` is used by ``OzoneCliHook`` and ``OzoneFsHook`` by default.
* ``ozone_admin_default`` is used by ``OzoneAdminHook`` by default.

Connection fields
-----------------

Host (required in Ozone CLI scope)
    Ozone Manager hostname for CLI endpoint wiring.

Port (required in Ozone CLI scope)
    Ozone Manager port for CLI endpoint wiring.

Extra
    JSON dictionary with canonical provider keys. Built-in provider code reads
    only fields defined in ``OzoneConnSnapshot``.

    Additional custom keys are allowed and available to DAG developers through
    ``hook.connection_snapshot.raw_extra``.

Connection extra keys (canonical)
---------------------------------

Ozone SSL/TLS scope:

* ``ozone_security_enabled``
* ``ozone_om_https_port``
* ``ozone_ssl_keystore_location``
* ``ozone_ssl_keystore_password`` (supports ``secret://...``)
* ``ozone_ssl_keystore_type``
* ``ozone_ssl_truststore_location``
* ``ozone_ssl_truststore_password`` (supports ``secret://...``)
* ``ozone_ssl_truststore_type``

Ozone Kerberos scope:

* ``hadoop_security_authentication`` (set ``kerberos`` to enable)
* ``kerberos_principal``
* ``kerberos_keytab`` (supports ``secret://...``)
* ``kerberos_password`` (supports ``secret://...``)
* ``krb5_conf`` (optional)
* ``ozone_conf_dir`` (recommended for all modes)

Connection extra by scenario
----------------------------

Use one of the following templates depending on your runtime mode.

1) Plain Ozone (no SSL, no Kerberos)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

  {
    "ozone_conf_dir": "/opt/airflow/ozone-conf"
  }

2) Ozone with SSL/TLS
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

  {
    "ozone_security_enabled": "true",
    "ozone_om_https_port": "9879",
    "ozone_ssl_keystore_location": "/opt/airflow/ssl/ozone-keystore.jks",
    "ozone_ssl_keystore_password": "secret://vault/ozone/keystore_password",
    "ozone_ssl_keystore_type": "JKS",
    "ozone_ssl_truststore_location": "/opt/airflow/ssl/ozone-truststore.jks",
    "ozone_ssl_truststore_password": "secret://vault/ozone/truststore_password",
    "ozone_ssl_truststore_type": "JKS",
    "ozone_conf_dir": "/opt/airflow/ozone-conf"
  }

3) Ozone with SSL/TLS and Kerberos using keytab
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

  {
    "ozone_security_enabled": "true",
    "ozone_om_https_port": "9879",
    "ozone_ssl_keystore_location": "/opt/airflow/ssl/ozone-keystore.jks",
    "ozone_ssl_keystore_password": "secret://vault/ozone/keystore_password",
    "ozone_ssl_keystore_type": "JKS",
    "ozone_ssl_truststore_location": "/opt/airflow/ssl/ozone-truststore.jks",
    "ozone_ssl_truststore_password": "secret://vault/ozone/truststore_password",
    "ozone_ssl_truststore_type": "JKS",
    "hadoop_security_authentication": "kerberos",
    "kerberos_principal": "testuser@EXAMPLE.COM",
    "kerberos_keytab": "/opt/airflow/keytabs/testuser.keytab",
    "krb5_conf": "/opt/airflow/kerberos-config/krb5.conf",
    "ozone_conf_dir": "/opt/airflow/ozone-conf"
  }

4) Ozone with SSL/TLS and Kerberos using password
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: json

  {
    "ozone_security_enabled": "true",
    "ozone_om_https_port": "9879",
    "ozone_ssl_keystore_location": "/opt/airflow/ssl/ozone-keystore.jks",
    "ozone_ssl_keystore_password": "secret://vault/ozone/keystore_password",
    "ozone_ssl_keystore_type": "JKS",
    "ozone_ssl_truststore_location": "/opt/airflow/ssl/ozone-truststore.jks",
    "ozone_ssl_truststore_password": "secret://vault/ozone/truststore_password",
    "ozone_ssl_truststore_type": "JKS",
    "hadoop_security_authentication": "kerberos",
    "kerberos_principal": "testuser@EXAMPLE.COM",
    "kerberos_password": "secret://vault/ozone/kerberos_password",
    "krb5_conf": "/opt/airflow/kerberos-config/krb5.conf",
    "ozone_conf_dir": "/opt/airflow/ozone-conf"
  }

Field-by-field explanation for SSL + Kerberos extra
----------------------------------------------------

``ozone_security_enabled``
    Enables Ozone security mode in provider runtime. Use ``"true"`` for SSL and
    SSL+Kerberos scenarios.

``ozone_om_https_port``
    HTTPS port used by Ozone OM in your cluster (for example ``9879`` in local
    demo setups). Must match actual OM SSL config.

``ozone_ssl_keystore_location`` / ``ozone_ssl_truststore_location``
    Absolute paths inside the Airflow worker runtime to keystore and truststore
    files used by Ozone CLI/JVM SSL layer.

``ozone_ssl_keystore_password`` / ``ozone_ssl_truststore_password``
    Passwords for the stores above. Prefer ``secret://...`` values so passwords
    are resolved from Airflow secrets backend instead of hardcoding plain text.

``ozone_ssl_keystore_type`` / ``ozone_ssl_truststore_type``
    Store format (typically ``JKS``). Must match how files were created.

``hadoop_security_authentication``
    Kerberos switch for provider runtime. Set exactly to ``"kerberos"`` to
    enable Kerberos env wiring and ``kinit`` flow.

``kerberos_principal``
    Principal used by tasks before CLI calls (for example
    ``testuser@EXAMPLE.COM``).

``kerberos_keytab``
    Absolute path to a keytab file inside the Airflow worker runtime.
    The file must exist and be readable by the task process. When both
    ``kerberos_keytab`` and ``kerberos_password`` are provided, the provider
    keeps the existing keytab-based flow.

``kerberos_password``
    Password for ``kerberos_principal``. Prefer ``secret://...`` values so the
    password is resolved from Airflow secrets backend. The provider uses the
    password only for ``kinit`` stdin and does not export it to Ozone CLI
    subprocess environment.

``krb5_conf``
    Optional explicit path to ``krb5.conf``. Recommended in containerized runs
    to avoid relying on container-image defaults.

``ozone_conf_dir``
    Directory with Ozone/Hadoop client configs (usually ``ozone-site.xml`` and
    ``core-site.xml``). This is one of the most important fields: provider uses
    it to set ``OZONE_CONF_DIR`` and (when needed) ``HADOOP_CONF_DIR``.

``host`` and ``port`` still matter even when ``ozone_conf_dir`` points to a
valid runtime config directory: the provider uses them to prepare CLI endpoint
wiring for task execution.

Common validation checklist for DAG developers
----------------------------------------------

* Paths in extra must be valid inside the worker runtime, not on your laptop.
* SSL store files and keytab files, when configured, must be mounted/readable
  for Airflow task user.
* Kerberos requires ``kerberos_principal`` plus either ``kerberos_keytab`` or
  ``kerberos_password``.
* ``ozone_conf_dir`` must contain config files pointing to real OM/SCM endpoints.

Why ``ozone_conf_dir`` is important
-----------------------------------

``ozone_conf_dir`` points to the directory inside the worker runtime where
Ozone/Hadoop client configs are available (usually ``ozone-site.xml`` and
``core-site.xml``).

The provider uses this value to prepare CLI runtime environment
(``OZONE_CONF_DIR`` and, when needed, ``HADOOP_CONF_DIR``), so ``ozone sh`` /
``ozone fs`` can resolve OM/SCM addresses and security policy.

If ``ozone_conf_dir`` is missing or points to the wrong path, commands may fail
even with correct connection host/port (for example: unresolved endpoints,
auth/security initialization failures, or generic Ozone CLI connection errors).

HDFS DistCp transfer scope (when ``hdfs_conn_id`` is provided)
---------------------------------------------------------------

``HdfsToOzoneOperator`` reads HDFS-specific security keys from the connection referenced by
``hdfs_conn_id`` and applies them only to DistCp subprocess environment.

HDFS SSL keys:

* ``hdfs_ssl_enabled``
* ``dfs_encrypt_data_transfer``
* ``hdfs_ssl_keystore_location``
* ``hdfs_ssl_keystore_password`` (supports ``secret://...``)
* ``hdfs_ssl_truststore_location``
* ``hdfs_ssl_truststore_password`` (supports ``secret://...``)

HDFS Kerberos keys:

* ``hdfs_kerberos_enabled``
* ``hdfs_kerberos_principal``
* ``hdfs_kerberos_keytab`` (supports ``secret://...``)
* ``hdfs_kerberos_password`` (supports ``secret://...``)
* optional ``krb5_conf`` for explicit Kerberos config path
* optional ``hdfs_distcp_mapreduce_local`` to run DistCp through local MapReduce
  when no YARN/MapReduce cluster config is available on the worker
* optional ``hdfs_distcp_renewer_principal`` to pass a Kerberos renewer principal
  to DistCp delegation token setup

For HDFS DistCp Kerberos, provide ``hdfs_kerberos_principal`` plus either
``hdfs_kerberos_keytab`` or ``hdfs_kerberos_password``. When both credentials
are configured, the provider keeps the existing keytab-based flow. Passwords
are used only for ``kinit`` stdin and are not exported to the DistCp subprocess
environment.

Example HDFS Kerberos extra for ``HdfsToOzoneOperator`` using password:

.. code-block:: json

  {
    "hdfs_kerberos_enabled": "true",
    "hdfs_kerberos_principal": "hdfs@EXAMPLE.COM",
    "hdfs_kerberos_password": "secret://vault/hdfs/kerberos_password",
    "hdfs_distcp_mapreduce_local": "true",
    "hdfs_distcp_renewer_principal": "hdfs@EXAMPLE.COM",
    "krb5_conf": "/opt/airflow/kerberos-config/krb5.conf"
  }

Recommended runtime notes
-------------------------

* Set ``ozone_conf_dir`` explicitly in connection extra for plain/SSL/Kerberos modes.
* Keep all security-sensitive values in Airflow Connection/Secrets backend,
  not in DAG source code.
* Do not rely on key aliases: only canonical key names are supported.

Recommended Kerberos extra (explicit paths):

.. code-block:: json

  {
    "ozone_security_enabled": "true",
    "hadoop_security_authentication": "kerberos",
    "kerberos_principal": "testuser@EXAMPLE.COM",
    "kerberos_keytab": "/opt/airflow/keytabs/testuser.keytab",
    "krb5_conf": "/opt/airflow/kerberos-config/krb5.conf",
    "ozone_conf_dir": "/opt/airflow/ozone-conf"
  }

Recommended Kerberos extra (password from secrets backend):

.. code-block:: json

  {
    "ozone_security_enabled": "true",
    "hadoop_security_authentication": "kerberos",
    "kerberos_principal": "testuser@EXAMPLE.COM",
    "kerberos_password": "secret://vault/ozone/kerberos_password",
    "krb5_conf": "/opt/airflow/kerberos-config/krb5.conf",
    "ozone_conf_dir": "/opt/airflow/ozone-conf"
  }

Upload/download size limit
--------------------------

The provider supports a connection-level limit for payload/file transfers:

* ``max_content_size_bytes`` (default: ``1073741824`` = 1 GiB)

This value is read from ``OzoneConnSnapshot`` and is used by:

* ``OzoneUploadContentOperator``
* ``OzoneUploadFileOperator``
* ``OzoneDownloadFileOperator`` (via ``OzoneFsHook.get_key_property(...)[\"data_size\"]``)

Each of these operators also accepts an optional ``max_content_size_bytes`` parameter
to override the connection-level value for that task.

Testing connections in Airflow UI
---------------------------------

The ``ozone`` connection type implements ``test_connection()``:

* ``ozone``: runs a minimal CLI command against Ozone Manager
  and returns Airflow-compatible status/message.
