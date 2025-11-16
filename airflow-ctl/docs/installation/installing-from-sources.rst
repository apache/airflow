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

Installing from Sources
-----------------------

Released packages
'''''''''''''''''

.. jinja:: official_download_page

    This page describes downloading and verifying Airflow Ctl version ``|version|`` using officially released packages.
    You can also install ``airflowctl`` - as most Python packages - via :doc:`PyPI <installing-from-pypi>`.
    You can choose different version of Airflow by selecting a different version from the drop-down at
    the top-left of the page.

The ``source``, ``sdist`` and ``whl`` packages released are the "official" sources of installation that you
can use if you want to verify the origin of the packages and want to verify checksums and signatures of
the packages. The packages are available via the
`Official Apache Software Foundations Downloads <https://dlcdn.apache.org/>`_

The {{ airflowctl_version }} downloads of Airflow Ctl are available at:

.. jinja:: official_download_page

    * `Sources package for airflow-ctl: <{{ closer_lua_url }}/apache_airflow_ctl-{{ airflowctl_version }}-source.tar.gz>`__ (`asc <{{ base_url }}/apache_airflow_ctl-{{ airflowctl_version }}-source.tar.gz.asc>`__, `sha512 <{{ base_url }}/apache_airflow_ctl-{{ airflowctl_version }}-source.tar.gz.sha512>`__)
    * `Sdist package for airflow-ctl distributions <{{ closer_lua_url }}/apache_airflow_ctl-{{ airflowctl_version }}.tar.gz>`__ (`asc <{{ base_url }}/apache_airflow_ctl-{{ airflowctl_version }}.tar.gz.asc>`__, `sha512 <{{ base_url }}/apache_airflow_ctl-{{ airflowctl_version }}.tar.gz.sha512>`__)
    * `Whl package for airflow-ctl distribution <{{ closer_lua_url }}/apache_airflow_ctl-{{ airflowctl_version }}-py3-none-any.whl>`__ (`asc <{{ base_url }}/apache_airflow_ctl-{{ airflowctl_version }}-py3-none-any.whl.asc>`__, `sha512 <{{ base_url }}/apache_airflow_ctl-{{ airflowctl_version }}-py3-none-any.whl.sha512>`__)

If you want to install from the source code, you can download from the sources link above, it will contain
a ``INSTALL`` file containing details on how you can build and install airflowctl.

Release integrity
'''''''''''''''''

`PGP signatures KEYS <https://downloads.apache.org/airflowctl/KEYS>`__

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

    gpg --verify apache-airflow-ctl-********.asc apache-airflow-ctl-*********

or

.. code-block:: bash

    pgpv apache-airflow-ctl-********.asc

or

.. code-block:: bash

    pgp apache-airflow-********.asc

Example:

.. code-block:: console
    :substitutions:

    $ gpg --verify apache-airflow-ctl-|version|-source.tar.gz.asc apache-airflow-ctl-|version|-source.tar.gz
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

    shasum -a 512 apache-airflow-ctl--********  | diff - apache-airflow-ctl--********.sha512

The ``SHASUM`` of the file should match the one provided in ``.sha512`` file.

Example:

.. code-block:: bash
    :substitutions:

    shasum -a 512 apache-airflow-ctl-|version|-source.tar.gz  | diff - apache-airflow-ctl-|version|-source.tar.gz.sha512


Verifying PyPI releases
'''''''''''''''''''''''

You can verify the airflowctl ``.whl`` packages from PyPI by locally downloading the package and signature
and SHA sum files with the script below:


.. jinja:: official_download_page

    .. code-block:: bash

        #!/bin/bash
        airflowctl_version="{{ airflowctl_version }}"
        ctl_download_dir="$(mktemp -d)"
        pip download --no-deps "apache-airflow-ctl==${airflowctl_version}" --dest "${airflow_download_dir}"
        curl "https://downloads.apache.org/airflowctl/${airflowctl_version}/apache_airflow_ctl-${airflowctl_version}-py3-none-any.whl.asc" \
            -L -o "${airflowctl_download_dir}/apache_airflow_ctl-${airflowctl_version}-py3-none-any.whl.asc"
        curl "https://downloads.apache.org/airflow/${airflowctl_version}/apache_airflow_ctl-${airflowctl_version}-py3-none-any.whl.sha512" \
            -L -o "${airflowctl_download_dir}/apache_airflow_ctl-${airflowctl_version}-py3-none-any.whl.sha512"
        echo
        echo "Please verify files downloaded to ${airflowctl_download_dir}"
        ls -la "${airflowctl_download_dir}"
        echo

Once you verify the files following the instructions from previous chapter you can remove the temporary
folder created.
