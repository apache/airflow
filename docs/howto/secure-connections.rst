..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Securing Connections
====================

By default, Airflow will save the passwords for the connection in plain text
within the metadata database. The ``crypto`` package is highly recommended
during installation. The ``crypto`` package does require that your operating
system has ``libffi-dev`` installed.

If ``crypto`` package was not installed initially, it means that your Fernet key in ``airflow.cfg`` is empty.

You can still enable encryption for passwords within connections by following below steps:

#. Install crypto package ``pip install apache-airflow[crypto]``
#. Generate fernet_key, using this code snippet below. ``fernet_key`` must be a base64-encoded 32-byte key:

    .. code:: python

      from cryptography.fernet import Fernet
      fernet_key= Fernet.generate_key()
      print(fernet_key.decode()) # your fernet_key, keep it in secured place!

#. Replace ``airflow.cfg`` fernet_key value with the one from `Step 2`. *Alternatively,* you can store your fernet_key in OS environment variable - You do not need to change ``airflow.cfg`` in this case as Airflow will use environment variable over the value in ``airflow.cfg``:

    .. code-block:: bash

      # Note the double underscores
      export AIRFLOW__CORE__FERNET_KEY=your_fernet_key

#. Restart the webserver
#. For existing connections (the ones that you had defined before installing ``airflow[crypto]`` and creating a Fernet key), you need to open each connection in the connection admin UI, re-type the password, and save the change

Rotating encryption keys
========================

Once connection credentials and variables have been encrypted using a fernet
key, changing the key will cause decryption of existing credentials to fail. To
rotate the fernet key without invalidating existing encrypted values, prepend
the new key to the ``fernet_key`` setting, run
``airflow rotate_fernet_key``, and then drop the original key from
``fernet_keys``:

#. Set ``fernet_key`` to ``new_fernet_key,old_fernet_key``
#. Run ``airflow rotate_fernet_key`` to re-encrypt existing credentials with the new fernet key
#. Set ``fernet_key`` to ``new_fernet_key``
