Securing Connections
====================

By default, Airflow will save the passwords for the connection in plain text
within the metadata database. The ``crypto`` package is highly recommended
during installation. The ``crypto`` package does require that your operating
system have libffi-dev installed.

If ``crypto`` package was not installed initially, you can still enable encryption for
connections by following steps below:

1. Install crypto package ``pip install apache-airflow[crypto]``
2. Generate fernet_key, using this code snippet below. fernet_key must be a base64-encoded 32-byte key.

.. code:: python

    from cryptography.fernet import Fernet
    fernet_key= Fernet.generate_key()
    print(fernet_key.decode()) # your fernet_key, keep it in secured place!

3. Replace ``airflow.cfg`` fernet_key value with the one from step 2.
Alternatively, you can store your fernet_key in OS environment variable. You
do not need to change ``airflow.cfg`` in this case as Airflow will use environment
variable over the value in ``airflow.cfg``:

.. code-block:: bash

  # Note the double underscores
  export AIRFLOW__CORE__FERNET_KEY=your_fernet_key

4. Restart Airflow webserver.
5. For existing connections (the ones that you had defined before installing ``airflow[crypto]`` and creating a Fernet key), you need to open each connection in the connection admin UI, re-type the password, and save it.

.. _secure-connections-kms:

Encrypting Connections with a KMS
---------------------------------

In addition to using a fernet_key, Airflow supports encrypting connections using keys 
managed by a Key Management System. In order to encrypt a connection using a KMS, follow these steps:

1. Setup or create an account on a KMS of your choice. Create at least one encryption/decryption key.
2. Install crypto package ``pip install apache-airflow[crypto]``
3. Create a regular (non-KMS encrypted) connection defining how to connect to your KMS.
4. Create (or modify) the connection you wish to encrypt. Choose the connection from step 3 for the  "KMS Conn Id" field, and fill out the any additional fields that appear.