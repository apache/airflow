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

Dependencies
------------

Airflow extra dependencies
''''''''''''''''''''''''''

The ``apache-airflow`` PyPI basic package only installs what's needed to get started.
Additional packages can be installed depending on what will be useful in your
environment. For instance, if you don't need connectivity with Postgres,
you won't have to go through the trouble of installing the ``postgres-devel``
yum package, or whatever equivalent applies on the distribution you are using.

Most of the extra dependencies are linked to a corresponding provider package. For example "amazon" extra
has a corresponding ``apache-airflow-providers-amazon`` provider package to be installed. When you install
Airflow with such extras, the necessary provider packages are installed automatically (latest versions from
PyPI for those packages). However, you can freely upgrade and install provider packages independently from
the main Airflow installation.

For the list of the extras and what they enable, see: :doc:`/extra-packages-ref`.

Provider packages
'''''''''''''''''

Unlike Apache Airflow 1.10, the Airflow 2.0 is delivered in multiple, separate, but connected packages.
The core of Airflow scheduling system is delivered as ``apache-airflow`` package and there are around
60 provider packages which can be installed separately as so called ``Airflow Provider packages``.
The default Airflow installation doesn't have many integrations and you have to install them yourself.

You can even develop and install your own providers for Airflow. For more information,
see: :doc:`apache-airflow-providers:index`

For the list of the provider packages and what they enable, see: :doc:`apache-airflow-providers:packages-ref`.

Differences between extras and providers
''''''''''''''''''''''''''''''''''''''''

Just to prevent confusion of extras versus provider packages: Extras and providers are different things,
though many extras are leading to installing providers.

Extras are standard Python setuptools feature that allows to add additional set of dependencies as
optional features to "core" Apache Airflow. One of the type of such optional features are providers
packages, but not all optional features of Apache Airflow have corresponding providers.

We are using the ``extras`` setuptools features to also install provider packages.
Most of the extras are also linked (same name) with provider packages - for example adding ``[google]``
extra also adds ``apache-airflow-providers-google`` as dependency. However, there are some extras that do
not install providers (examples ``github_enterprise``, ``kerberos``, ``async`` - they add some extra
dependencies which are needed for those ``extra`` features of Airflow mentioned. The three examples
above add respectively GitHub Enterprise OAuth authentication, Kerberos integration or
asynchronous workers for Gunicorn. None of those have providers, they are just extending Apache Airflow
"core" package with new functionalities.

System dependencies
'''''''''''''''''''

You need certain system level requirements in order to install Airflow.
Those are requirements that are known to be needed for Linux Debian distributions:

Debian Bookworm (12)
====================

Debian Bookworm is our platform of choice for development and testing. It is the most up-to-date
Debian distribution and it is the one we use for our CI/CD system. It is also the one we recommend
for development and testing as well as production use.

.. code-block:: bash

  sudo apt install -y --no-install-recommends apt-transport-https apt-utils ca-certificates \
    curl dumb-init freetds-bin gosu krb5-user libgeos-dev \
    ldap-utils libsasl2-2 libsasl2-modules libxmlsec1 locales libffi8 libldap-2.5-0 libssl3 netcat-openbsd \
    lsb-release openssh-client python3-selinux rsync sasl2-bin sqlite3 sudo unixodbc

Debian Bullseye (11)
====================

Debian Bullseye is the previous Debian distribution. It is still supported by Airflow and it is
the one we also recommend for production use, however we only build images in the CI and we do not
run any tests there (we do not expect problems though). In Airflow 2.9 we are going to stop building images
for Bullseye and we will only build images and explain system level dependencies for Bookworm.

.. code-block:: bash

  sudo apt install -y --no-install-recommends apt-transport-https apt-utils ca-certificates \
    curl dumb-init freetds-bin gosu krb5-user libgeos-dev \
    ldap-utils libsasl2-2 libsasl2-modules libxmlsec1 locales libffi7 libldap-2.4-2 libssl1.1 netcat \
    lsb-release openssh-client python3-selinux rsync sasl2-bin sqlite3 sudo unixodbc

You also need database client packages (Postgres or MySQL) if you want to use those databases.


If you use a different distribution, you will need to adapt the commands accordingly.
