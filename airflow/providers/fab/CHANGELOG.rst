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

``apache-airflow-providers-fab``

Changelog
---------

1.4.0
.....

Features
~~~~~~~~

* ``Add FAB migration commands (#41804)``
* ``Separate FAB migration from Core Airflow migration (#41437)``

Misc
~~~~

* ``Deprecated kerberos auth removed (#41693)``
* ``Deprecated configuration removed (#42129)``
* ``Move 'is_active' user property to FAB auth manager (#42042)``
* ``Move 'register_views' to auth manager interface (#41777)``
* ``Revert "Provider fab auth manager deprecated methods removed (#41720)" (#41960)``
* ``Provider fab auth manager deprecated methods removed (#41720)``
* ``Make kerberos an optional and devel dependency for impala and fab (#41616)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add TODOs in providers code for Subdag code removal (#41963)``
   * ``Add fixes by breeze/precommit-lint static checks (#41604) (#41618)``

.. Review and move the new changes to one of the sections above:
   * ``Fix pre-commit for auto update of fab migration versions (#42382)``
   * ``Handle 'AUTH_ROLE_PUBLIC' in FAB auth manager (#42280)``

1.3.0
.....

Features
~~~~~~~~

* ``Feature: Allow set Dag Run resource into Dag Level permission (#40703)``

Misc
~~~~

* ``Remove deprecated SubDags (#41390)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.2.2
.....

Bug Fixes
~~~~~~~~~

* ``Bug fix: sync perm command not able to use custom security manager (#41020)``
* ``Bump version checked by FAB provider on logout CSRF protection to 2.10.0 (#40784)``

Misc
~~~~

* ``AIP-44 make database isolation mode work in Breeze (#40894)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Add backward compatibility to CSRF protection of '/logout' method (#40479)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

1.2.0
.....

Features
~~~~~~~~

* ``Add CSRF protection to "/logout" (#40145)``

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``
* ``Upgrade to FAB 4.5.0 (#39851)``
* ``fix: sqa deprecations for airflow providers (#39293)``
* ``Add '[webserver]update_fab_perms' to deprecated configs (#40317)``

1.1.1
.....

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``
* ``Simplify action name retrieval in FAB auth manager (#39358)``
* ``Add 'jmespath' as an explicit dependency (#39350)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

1.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Remove plugins permissions from Viewer role (#39254)``
* ``Update 'is_authorized_custom_view' from auth manager to handle custom actions (#39167)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

1.0.4
.....

Bug Fixes
~~~~~~~~~

* ``Remove button for reset my password when we have reset password (#38957)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Activate RUF019 that checks for unnecessary key check (#38950)``


1.0.3
.....

Bug Fixes
~~~~~~~~~

* ``Rename 'allowed_filter_attrs' to 'allowed_sort_attrs' (#38626)``
* ``Fix azure authentication when no email is set (#38872)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``fix: try002 for provider fab (#38801)``

1.0.2
.....

First stable release for the provider


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade FAB to 4.4.1 (#38319)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Make the method 'BaseAuthManager.is_authorized_custom_view' abstract (#37915)``
   * ``Avoid use of 'assert' outside of the tests (#37718)``
   * ``Resolve G004: Logging statement uses f-string (#37873)``
   * ``Remove useless methods from security manager (#37889)``
   * ``Use 'next' when redirecting (#37904)``
   * ``Add "MENU" permission in auth manager (#37881)``
   * ``Avoid to use too broad 'noqa' (#37862)``
   * ``Add post endpoint for dataset events (#37570)``
   * ``Add "queuedEvent" endpoint to get/delete DatasetDagRunQueue (#37176)``
   * ``Add swagger path to FAB Auth manager and Internal API (#37525)``
   * ``Revoking audit_log permission from all users except admin (#37501)``
   * ``Enable the 'Is Active?' flag by default in user view (#37507)``
   * ``Add comment about versions updated by release manager (#37488)``
   * ``Until we release 2.9.0, we keep airflow >= 2.9.0.dev0 for FAB provider (#37421)``
   * ``Improve suffix handling for provider-generated dependencies (#38029)``

1.0.0 (YANKED)
..............

Initial version of the provider (beta).
