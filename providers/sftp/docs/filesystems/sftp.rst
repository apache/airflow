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

SFTP Filesystem
===============

Use ``ObjectStoragePath`` with SFTP/SSH servers via the `sshfs <https://github.com/fsspec/sshfs>`__ library.

.. code-block:: bash

    pip install apache-airflow-providers-sftp[sshfs]

URL format: ``sftp://connection_id@hostname/path/to/file`` (also supports ``ssh://``).

Configuration
-------------

Uses the standard SFTP connection. The following extras are supported:

* ``key_file`` - path to private key file
* ``private_key`` - private key content (PEM format)
* ``private_key_passphrase`` - passphrase for encrypted keys
* ``no_host_key_check`` - set to ``true`` to skip host key verification

See :doc:`/connections/sftp` for details.

Example
-------

.. code-block:: python

    from airflow.sdk import ObjectStoragePath

    path = ObjectStoragePath("sftp://my_conn@myserver/data/file.csv")

    # read
    with path.open() as f:
        data = f.read()

    # write
    with path.open("w") as f:
        f.write("content")

    # list
    for p in path.parent.iterdir():
        print(p.name)

    # copy
    path.copy(ObjectStoragePath("file:///tmp/local.csv"))
