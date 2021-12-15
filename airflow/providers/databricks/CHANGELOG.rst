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

2.1.0
.....

Features
~~~~~~~~

* ``Databricks: add more methods to represent run state information (#19723)``
* ``Databricks - allow Azure SP authentication on other Azure clouds (#19722)``
* ``Databricks: allow to specify PAT in Password field (#19585)``
* ``Databricks jobs 2.1 (#19544)``
* ``Update Databricks API from 2.0 to 2.1 (#19412)``
* ``Authentication with AAD tokens in Databricks provider (#19335)``
* ``Update Databricks operators to match latest version of API 2.0 (#19443)``
* ``Remove db call from DatabricksHook.__init__() (#20180)``

Bug Fixes
~~~~~~~~~

* ``Fixup string concatenations (#19099)``
* ``Databricks hook: fix expiration time check (#20036)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``Refactor DatabricksHook (#19835)``
   * ``Update documentation for November 2021 provider&#39;s release (#19882)``
   * ``Unhide changelog entry for databricks (#20128)``
   * ``Update documentation for RC2 release of November Databricks Provider (#20086)``

2.0.2
.....

Bug Fixes
~~~~~~~~~
   * ``Move DB call out of DatabricksHook.__init__ (#18339)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``

2.0.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Prepares docs for Rc2 release of July providers (#17116)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Removes pylint from our toolchain (#16682)``

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
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``An initial rework of the 'Concepts' docs (#15444)``
   * ``Remove Backport Providers (#14886)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Add documentation for Databricks connection (#15410)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.1
.....

Updated documentation and readme files.

1.0.0
.....

Initial version of the provider.
