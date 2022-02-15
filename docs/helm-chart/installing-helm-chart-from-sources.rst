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

Installing Helm Chart from sources
----------------------------------

Released packages
'''''''''''''''''

.. jinja:: official_download_page

    This page describes downloading and verifying ``Apache Airflow Official Helm Chart`` version
    ``{{ package_version}}`` using officially released source packages. You can also install the chart
    directly from the ``airflow.apache.org`` repo as described in
    `Installing the chart <index#installing-the-chart>`_.
    You can choose different version of the chart by selecting different version from the drop-down at
    the top-left of the page.


The sources and packages released are the "official" sources of installation that you can use if
you want to verify the origin of the packages and want to verify checksums and signatures of the packages.
The packages are available via the
`Official Apache Software Foundations Downloads <http://ws.apache.org/mirrors.cgi>`_

The downloads are available at:

.. jinja:: official_download_page

    * `Sources package <{{ closer_lua_url }}/{{ package_version }}/airflow-chart-{{ package_version }}-source.tar.gz>`__ (`asc <{{ base_url }}/{{ package_version }}/airflow-chart-{{ package_version }}-source.tar.gz.asc>`__, `sha512 <{{ base_url }}/{{ package_version }}/airflow-chart-{{ package_version }}-source.tar.gz.sha512>`__)
    * `Installable package <{{ closer_lua_url }}/{{ package_version }}/airflow-{{ package_version }}.tgz>`__ (`asc <{{ base_url }}/{{ package_version }}/airflow-{{ package_version }}.tgz.asc>`__, `sha512 <{{ base_url }}/{{ package_version }}/airflow-{{ package_version }}.tgz.sha512>`__)

If you want to install from the source code, you can download from the sources link above, it will contain
a ``INSTALL`` file containing details on how you can build and install the chart.

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

    gpg --verify airflow-********.asc airflow-*********

or

.. code-block:: bash

    pgpv airflow-********.asc

or

.. code-block:: bash

    pgp airflow-********.asc

Example:

.. jinja:: official_download_page

    .. code-block:: console
        :substitutions:

        $ gpg --verify airflow-{{ package_version }}.tgz.asc airflow-{{ package_version }}.tgz
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

        shasum -a 512 airflow-********  | diff - airflow-********.sha512

    The ``SHASUM`` of the file should match the one provided in ``.sha512`` file.

    Example:

    .. code-block:: bash
        :substitutions:

        shasum -a 512 airflow-{{ package_version }}.tgz  | diff - airflow-{{ package_version }}.tgz.sha512
