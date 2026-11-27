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

FTP Filesystem
==============

Use ``ObjectStoragePath`` with FTP servers through the FTP implementation that ships with
`fsspec <https://filesystem-spec.readthedocs.io/>`__. No extra package is needed.

URL format: ``ftp://connection_id@hostname/path/to/file``.

Configuration
-------------

Uses the standard FTP connection: host, port (21 by default), login and password. The following
extras are supported:

* ``passive`` - set to ``false`` to use active mode. Passive mode is on by default.
* ``encoding`` - encoding of the file names on the server, for example ``cp1251``.

FTPS is not supported.

See :doc:`/connections/ftp` for details.

Example
-------

.. code-block:: python

    from airflow.sdk import ObjectStoragePath

    path = ObjectStoragePath("ftp://my_conn@myserver/data/file.csv")

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
