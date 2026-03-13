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


Installing Providers from Sources
---------------------------------

Released packages
'''''''''''''''''

.. jinja:: official_download_page

    The Source packages released last time are official packages of the Apache Software Foundation -
    and the ones that you can use is you want to build the packages yourself from the source code and be
    sure that the provenance of the packages is verified and matches the source code from the repository
    and you can verify the checksums and signatures of the packages.

    * `Sources package for providers <{{ closer_lua_url }}/apache_airflow_providers-{{ providers_release_date }}-source.tar.gz>`__ (`asc <{{ base_url }}/apache_airflow_providers-{{ providers_release_date }}-source.tar.gz.asc>`__, `sha512 <{{ base_url }}/apache_airflow_providers-{{ providers_release_date }}-source.tar.gz.sha512>`__)

    Here are individual download pages for all the released providers:

    .. raw:: html

        <ul style="column-count: 2;">
        {% for provider in all_providers %}
            <li><a href="/docs/{{ provider['package-name'] }}/stable/installing-providers-from-sources.html"><code>{{ provider.name }}</code></a></li>
        {% endfor %}
         </ul>


    You can also install ``Apache Airflow Providers`` - as most Python packages - via :doc:`PyPI <installing-from-pypi>`.
    You can choose different version of Airflow by selecting different version from the drop-down at
    the top-left of the page of the providers above.

Release integrity
'''''''''''''''''

`PGP signatures KEYS <https://downloads.apache.org/airflow/KEYS>`_

It is essential that you verify the integrity of the downloaded files using the PGP or SHA signatures.
The PGP signatures can be verified using GPG or PGP. Please download the KEYS as well as the asc
signature files for relevant distribution. It is recommended to get these files from the
main distribution directory and not from the mirrors.

.. code-block:: bash

    gpg -i KEYS

or

.. code-block:: bash

    pgpk -a KEYS

or

.. code-block:: bash

    pgp -ka KEYS

To verify the binaries/sources you can download the relevant asc files for it from main
distribution directory and follow the below guide.

.. code-block:: bash

    gpg --verify apache-airflow-providers-********.asc apache-airflow-providers-*********

or

.. code-block:: bash

    pgpv apache-airflow-providers-********.asc

or

.. code-block:: bash

    pgp apache-airflow-providers-********.asc

Example:

.. code-block:: console
    :substitutions:

    $ gpg --verify apache-airflow-providers-airbyte-1.0.0-source.tar.gz.asc apache-airflow-providers-airbyte-1.0.0-source.tar.gz
      gpg: Signature made Sat 11 Sep 12:49:54 2021 BST
      gpg:                using RSA key CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
      gpg:                issuer "kaxilnaik@apache.org"
      gpg: Good signature from "Kaxil Naik <kaxilnaik@apache.org>" [unknown]
      gpg:                 aka "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
      gpg: WARNING: The key's User ID is not certified with a trusted signature!
      gpg:          There is no indication that the signature belongs to the owner.
      Primary key fingerprint: CDE1 5C6E 4D3A 8EC4 ECF4  BA4B 6674 E08A D7DE 406F

The "Good signature from ..." is indication that the signatures are correct.
Do not worry about the "not certified with a trusted signature" warning. Most of the certificates used
by release managers are self signed, that's why you get this warning. By importing the server in the
previous step and importing it via ID from ``KEYS`` page, you know that this is a valid Key already.

For SHA512 sum check, download the relevant ``sha512`` and run the following:

.. code-block:: bash

    shasum -a 512 apache-airflow-providers-********  | diff - apache-airflow-providers-********.sha512

The ``SHASUM`` of the file should match the one provided in ``.sha512`` file.

Example:

.. code-block:: bash
    :substitutions:

    shasum -a 512 apache-airflow-providers-airbyte-1.0.0-source.tar.gz  | diff - apache-airflow-providers-airbyte-1.0.0-source.tar.gz.sha512
