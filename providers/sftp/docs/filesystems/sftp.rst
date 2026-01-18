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

SFTP / SSH Filesystem
=====================

The SFTP filesystem provides access to remote servers via SSH File Transfer Protocol (SFTP) through
Airflow's ``ObjectStoragePath`` interface. This allows you to perform file operations on remote SSH
servers using the same API as other object storage backends like S3 or GCS.

Supported URL formats:

* ``sftp://connection_id@hostname/path/to/file``
* ``ssh://connection_id@hostname/path/to/file``

Installation
------------

The SFTP filesystem requires the ``sshfs`` library. Install it with:

.. code-block:: bash

    pip install apache-airflow-providers-sftp[sshfs]

Connection Configuration
------------------------

The SFTP filesystem uses Airflow's SFTP connection type. Create a connection with the following parameters:

* **Connection Type**: sftp
* **Host**: Remote server hostname or IP address
* **Port**: SSH port (default: 22)
* **Login**: SSH username
* **Password**: SSH password (if using password authentication)

Additional configuration via connection extras:

* **key_file**: Path to the private SSH key file for key-based authentication
* **private_key**: Content of the private key (PEM format) for key-based authentication
* **private_key_passphrase**: Passphrase for the private key (if encrypted)
* **no_host_key_check**: Set to ``true`` to disable host key verification (not recommended for production)

For more details on connection configuration, see :doc:`/connections/sftp`.

Connection extra field configuration examples:

**Using password authentication:**

.. code-block:: json

    {}

**Using key file:**

.. code-block:: json

    {
        "key_file": "/path/to/private_key"
    }

**Using private key content:**

.. code-block:: json

    {
        "private_key": "<PEM-encoded private key content>",
        "private_key_passphrase": "optional_passphrase"
    }

**Disabling host key verification (use with caution):**

.. code-block:: json

    {
        "no_host_key_check": "true"
    }

Usage Examples
--------------

Basic File Operations
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from airflow.sdk import ObjectStoragePath

    # Access a file on a remote SFTP server
    path = ObjectStoragePath("sftp://my_sftp_conn@remote-server/home/user/data.csv")

    # Read file content
    with path.open("r") as f:
        content = f.read()

    # Write to a file
    output_path = ObjectStoragePath("sftp://my_sftp_conn@remote-server/home/user/output.txt")
    with output_path.open("w") as f:
        f.write("Hello from Airflow!")

Directory Operations
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # List directory contents
    remote_dir = ObjectStoragePath("sftp://my_sftp_conn@remote-server/home/user/data/")

    for item in remote_dir.iterdir():
        print(f"Found: {item.name}")
        if item.is_file():
            print(f"  Size: {item.stat().st_size} bytes")

    # Create a directory
    new_dir = ObjectStoragePath("sftp://my_sftp_conn@remote-server/home/user/new_folder/")
    new_dir.mkdir(parents=True, exist_ok=True)

Copying Files
^^^^^^^^^^^^^

.. code-block:: python

    # Copy from SFTP to local
    remote_file = ObjectStoragePath("sftp://my_sftp_conn@remote-server/data/input.csv")
    local_file = ObjectStoragePath("file:///tmp/input.csv")
    remote_file.copy(local_file)

    # Copy from local to SFTP
    local_output = ObjectStoragePath("file:///tmp/output.csv")
    remote_output = ObjectStoragePath("sftp://my_sftp_conn@remote-server/data/output.csv")
    local_output.copy(remote_output)

    # Copy between different SFTP servers
    source = ObjectStoragePath("sftp://server1_conn@server1/data/file.txt")
    target = ObjectStoragePath("sftp://server2_conn@server2/backup/file.txt")
    source.copy(target)

Cross-Backend Operations
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    # Copy from SFTP to S3
    sftp_file = ObjectStoragePath("sftp://my_sftp_conn@remote-server/exports/data.parquet")
    s3_file = ObjectStoragePath("s3://aws_conn@my-bucket/imports/data.parquet")
    sftp_file.copy(s3_file)

    # Copy from GCS to SFTP
    gcs_file = ObjectStoragePath("gs://gcp_conn@my-bucket/reports/report.pdf")
    sftp_target = ObjectStoragePath("sftp://my_sftp_conn@remote-server/incoming/report.pdf")
    gcs_file.copy(sftp_target)

Using in Tasks
^^^^^^^^^^^^^^

.. code-block:: python

    from airflow.sdk import ObjectStoragePath
    from airflow.decorators import task


    @task
    def process_remote_file(remote_path: ObjectStoragePath) -> dict:
        """Process a file from an SFTP server."""
        with remote_path.open("r") as f:
            content = f.read()

        return {
            "size": remote_path.stat().st_size,
            "lines": len(content.splitlines()),
        }


    @task
    def upload_results(local_path: ObjectStoragePath, remote_path: ObjectStoragePath):
        """Upload processed results to SFTP server."""
        local_path.copy(remote_path)

Storage Options
---------------

You can pass additional options to the underlying ``SSHFileSystem`` via storage options:

.. code-block:: python

    path = ObjectStoragePath(
        "sftp://my_sftp_conn@remote-server/data/file.txt",
        storage_options={
            "connect_timeout": 30,  # Connection timeout in seconds
        },
    )

Storage options are merged with connection settings, with storage options taking precedence.

Security Considerations
-----------------------

* **Host key verification**: By default, host key verification is enabled. Only disable it
  (``no_host_key_check: true``) in development environments or when you understand the security
  implications.

* **Key-based authentication**: Prefer key-based authentication over password authentication
  for better security.

* **Private key storage**: When using ``private_key`` in connection extras, ensure your Airflow
  metadata database is properly secured, as the key content is stored there.

Requirements
------------

The SFTP filesystem requires:

* ``sshfs`` Python package (``>=2023.1.0``)
* Valid SSH/SFTP server access
* Appropriate authentication credentials

Cross-References
----------------

* :doc:`/connections/sftp` - SFTP connection configuration
* :doc:`/sensors/sftp_sensor` - SFTP file sensors
* :doc:`apache-airflow:core-concepts/objectstorage` - ObjectStoragePath documentation

Reference
---------

For further information, see:

* `sshfs Python package <https://pypi.org/project/sshfs/>`__
* `asyncssh documentation <https://asyncssh.readthedocs.io/>`__
* `SFTP protocol specification <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
