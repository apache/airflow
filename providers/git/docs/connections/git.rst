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



.. _howto/connection:ftp:

GIT Connection
==============

The GIT connection type enables the GIT Integrations.

Authenticating to GIT
-----------------------

Authenticate to FTP using `ftplib
<https://docs.python.org/3/library/ftplib.html>`_.
i.e. indicate ``user``, ``password``, ``host``

Default Connection IDs
----------------------

Hooks, bundles related to GIT use ``git_default`` by default.

Configuring the Connection
--------------------------
Username
    Specify the git ``username``.

Repository URL (optional)
    Specify the repository url e.g ``git@github.com/apache/airflow.git``.

Password (optional)
    Specify the git ``password`` a.k.a ``ACCESS TOKEN`` if using https.

Extra (optional)
    Specify the extra parameters (as json dictionary) that can be used in ftp connection.
    You can specify the ``key_file`` path or ``private_key`` as extra parameters. You can
    also optionally specify the ``strict_host_key_checking`` parameter.
