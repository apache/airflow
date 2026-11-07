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

.. _howto/connection:sas:

SAS Connection
==============

The SAS connection type lets Airflow run ``PROC SQL`` on a SAS 9.4 server. The hook connects
to the server over SSH, uploads the SQL as a SAS program, runs the SAS binary and downloads
the result as a ``.sas7bdat`` file, which ``pandas`` parses with the column types stored in the
file. No JVM, JDBC driver or SAS client is needed on the worker.

Authenticating to SAS
---------------------

The SSH session can be opened in two ways:

* **Direct mode** (default): the SAS connection holds the SSH credentials itself (host, port,
  login, password), plus the optional ``host_key`` / ``no_host_key_check`` extras.
* **Delegated mode**: ``ssh_conn_id`` (hook argument or connection extra) names an
  :doc:`SSH connection <apache-airflow-providers-ssh:connections/ssh>`, which then provides host, user, private key and
  host-key settings. The SAS connection only carries the SAS settings below.

Default Connection IDs
----------------------

All hooks related to SAS use ``sas_default`` by default.

Configuring the Connection
--------------------------

Host
    Hostname of the SAS server, reachable over SSH (direct mode).

Port
    SSH port. Defaults to ``22`` (direct mode).

Login
    SSH user that runs SAS (direct mode).

Password
    Password of that user (direct mode). Use delegated mode for key-based authentication.

Extra (optional)
    A JSON dictionary. All keys are optional; a hook argument of the same name wins over the
    extra:

    * ``ssh_conn_id``: SSH connection to delegate authentication to.
    * ``sas_binary`` *(default ``/opt/sas/SASFoundation/9.4/sas``)*: path to the SAS binary.
    * ``sas_options`` *(default ``["-linesize 90", "-noterminal", "-nodate", "-nonews"]``)*:
      command-line options passed to SAS.
    * ``sas_libraries``: library name to its directory on the server, e.g.
      ``{"sales": "/data/sas/sales"}``. The hook writes one ``LIBNAME`` per library the SQL
      refers to (``FROM sales.orders``).
    * ``sas_temp_dir`` *(default ``/tmp``)*: server directory for the program, its log and the
      result. The hook removes them after each run.
    * ``sas_work_dir`` *(default: ``sas_temp_dir``)*: root of the SAS ``WORK`` library, for
      servers whose default ``WORK`` is not writable by the SSH user.
    * ``encoding`` *(default ``iso-8859-15``)*: encoding of the result file and the SAS log.
    * ``timeout``: seconds to wait for one SAS run before the statement fails.
    * ``sas_compress`` *(default ``yes``)*: ``COMPRESS=`` option of the result data set
      (``yes``, ``char``, ``binary`` or ``no``).
    * ``ssh_compress`` *(default ``true``)*: zlib on the SSH transport (direct mode).
    * ``keepalive_interval`` *(default ``30``)*: SSH keepalive in seconds, ``0`` disables it
      (direct mode).
    * ``host_key``: the server's public host key as in ``known_hosts`` without the host name
      (direct mode).
    * ``no_host_key_check``: ``true`` accepts any host key (direct mode). Not recommended.

Examples
--------

Direct mode, password authentication:

.. code-block:: bash

    export AIRFLOW_CONN_SAS_DEFAULT='{
        "conn_type": "sas",
        "host": "sas-server.example.com",
        "login": "sas_user",
        "password": "secret",
        "extra": {
            "sas_libraries": {"sales": "/data/sas/sales"},
            "sas_temp_dir": "/data/sas/work",
            "host_key": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA..."
        }
    }'

Delegated mode, the SSH connection holds the private key:

.. code-block:: bash

    export AIRFLOW_CONN_SAS_DEFAULT='{
        "conn_type": "sas",
        "extra": {
            "ssh_conn_id": "ssh_sas_server",
            "sas_libraries": {"sales": "/data/sas/sales"}
        }
    }'
