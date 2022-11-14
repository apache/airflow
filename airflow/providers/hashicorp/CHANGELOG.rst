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


.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.

Changelog
---------

3.2.0
.....

This release of provider is only available for Airflow 2.3+ as explained in the
`Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/README.md#support-for-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``
* ``Add Airflow specific warning classes (#25799)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Pass kwargs from vault hook to hvac client (#26680)``
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Update docs for September Provider's release (#26731)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

3.1.0
.....

Features
~~~~~~~~

* ``Use newer kubernetes authentication method in internal vault client (#25351)``


3.0.1
.....

Bug Fixes
~~~~~~~~~

* ``Update providers to use functools compat for ''cached_property'' (#24582)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare provider documentation 2022.05.11 (#23631)``
   * ``pydocstyle D202 added (#24221)``
   * ``Prepare docs for May 2022 provider's release (#24231)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update package description to remove double min-airflow specification (#24292)``

2.2.0
.....

Features
~~~~~~~~

* ``Update secrets backends to use get_conn_value instead of get_conn_uri (#22348)``

   * ``Prepare mid-April provider documentation. (#22819)``
   * ``Clean up in-line f-string concatenation (#23591)``
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``


2.1.4
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.1.3
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.1.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix Vault Hook default connection name (#20792)``

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Fix cached_property MyPy declaration and related MyPy errors (#20226)``
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.1.1
.....

Bug Fixes
~~~~~~~~~

* ``Fixing Vault AppRole authentication with CONN_URI (#18064)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.1.0
.....

Bug Fixes
~~~~~~~~~

* ``Fix instantiating Vault Secret Backend during configuration (#17935)``

Features
~~~~~~~~

* ``Invalidate Vault cached prop when not authenticated (#17387)``
* ``Enable Connection creation from Vault parameters (#15013)``

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``
* ``Adds secrets backend/logging/auth information to provider yaml (#17625)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Prepares docs for Rc2 release of July providers (#17116)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Removes pylint from our toolchain (#16682)``
   * ``Add August 2021 Provider's documentation (#17890)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

Bug Fixes
~~~~~~~~~

* ``Sanitize end of line character when loading token from a file (vault) (#16407)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix deprecated warning hvac auth (#15216)``

1.0.1
.....

Updated documentation and readme files.

1.0.0
.....

Initial version of the provider.
