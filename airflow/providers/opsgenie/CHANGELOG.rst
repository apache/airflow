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

5.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* Remove 'OpsgenieAlertOperator' also removed hooks.opsgenie_alert path


4.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate Opsgenie example DAGs to new design #22455 (#24144)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

3.1.0
.....

Features
~~~~~~~~

* ``Add 'OpsgenieDeleteAlertOperator' (#23405)``

Bug Fixes
~~~~~~~~~

* ``Opsgenie: Fix 'close_alert' to properly send 'kwargs' (#23442)``

3.0.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

3.0.2
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

3.0.1
.....

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

``OpsgenieAlertHook`` constructor does not take additional arguments or keyword arguments anymore.
Changed the return type of ``OpsgenieAlertHook.get_conn`` to return an ``opsgenie_sdk.AlertApi`` object instead of a ``requests.Session`` object.
Removed the ``OpsegnieAlertHook.execute`` method and replaced it with ``OpsegnieAlertHook.create_alert`` it now returns an
``opsgenie_sdk.SuccessResponse`` object instead of an ``Any`` type.
``OpsgenieAlertHook`` now takes ``visible_to`` instead of ``visibleTo`` key in the payload.
``OpsgenieAlertHook`` now takes ``request_id`` instead of ``requestId`` key in the payload.

* ``Add 'OpsgenieCloseAlertOperator' (#20488)``
* ``Organize Opsgenie provider classes (#20454)``
* ``rewrite opsgenie alert hook with official python sdk, related issue #18641 (#20263)``
* ``Rename 'OpsgenieAlertOperator' to 'OpsgenieCreateAlertOperator' (#20514)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Update documentation for provider December 2021 release (#20523)``

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

Bug Fixes
~~~~~~~~~

* ``Fix hooks extended from http hook (#16109)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix type hints in OpsgenieAlertOperator (#14637)``

1.0.1
.....

Updated documentation and readme files.

1.0.0
.....

Initial version of the provider.
