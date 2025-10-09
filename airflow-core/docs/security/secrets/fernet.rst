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

.. _security/fernet:

Fernet
------

Airflow uses `Fernet <https://github.com/fernet/spec/>`__ to encrypt passwords in the connection
configuration and the variable configuration. It guarantees that a password encrypted using it cannot be manipulated or read without the key.
Fernet is an implementation of symmetric (also known as "secret key") authenticated cryptography.

The first time Airflow is started, the ``airflow.cfg`` file is generated with the default configuration and the unique Fernet
key. The key is saved to option ``fernet_key`` of section ``[core]``.

You can also configure a fernet key using environment variables. This will overwrite the value from the
``airflow.cfg`` file

    .. code-block:: bash

      # Note the double underscores
      export AIRFLOW__CORE__FERNET_KEY=your_fernet_key

Generating Fernet key
'''''''''''''''''''''

If you need to generate a new fernet key you can use the following code snippet.

    .. code-block:: python

      from cryptography.fernet import Fernet

      fernet_key = Fernet.generate_key()
      print(fernet_key.decode())  # your fernet_key, keep it in secured place!


Rotating encryption keys
''''''''''''''''''''''''

Once connection credentials and variables have been encrypted using a fernet
key, changing the key will cause decryption of existing credentials to fail. To
rotate the fernet key without invalidating existing encrypted values, prepend
the new key to the ``fernet_key`` setting, run
``airflow rotate-fernet-key``, and then drop the original key from
``fernet_key``:

#. Set ``fernet_key`` to ``new_fernet_key,old_fernet_key``
#. Run ``airflow rotate-fernet-key`` to re-encrypt existing credentials with the new fernet key
#. Set ``fernet_key`` to ``new_fernet_key``
