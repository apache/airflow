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


Changelog
---------

2.1.6
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.1.5
.....

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.1.4
.....

Misc
~~~~


* ``Squelch more deprecation warnings (#21003)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix MyPy Errors for providers: Tableau, CNCF, Apache (#20654)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Add documentation for January 2021 providers release (#21257)``

2.1.3
.....

Bug Fixes
~~~~~~~~~

* ``Ensure Tableau connection is active to access wait_for_state (#20433)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix MyPy Errors for Tableau provider (#20240)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.1.2
.....

Bug Fixes
~~~~~~~~~

* ``Remove distutils usages for Python 3.10 (#19064)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update documentation for September providers release (#18613)``
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``

2.1.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``New generic tableau operator: TableauOperator  (#16915)``

2.1.0
.....

Features
~~~~~~~~

* ``Allow disable SSL for TableauHook (#16365)``
* ``Deprecate Tableau personal token authentication (#16916)``
* ``Fix bool conversion Verify parameter in Tableau Hook (#17125)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
   * ``Refactored waiting function for Tableau Jobs (#17034)``
   * ``Remove/refactor default_args pattern for miscellaneous providers (#16872)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Adds interactivity when generating provider documentation. (#15518)``
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``Remove Backport Providers (#14886)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Add Connection Documentation for Providers (#15499)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.0
.....

Initial version of the provider.
