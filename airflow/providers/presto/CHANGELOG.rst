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
......

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  In this version of the provider, deprecated GCS hook's param ``delegate_to`` is removed from ``GCSToPrestoOperator``.
  Impersonation can be achieved instead by utilizing the ``impersonation_chain`` param.

* ``remove delegate_to from GCP operators and hooks (#30748)``

.. Review and move the new changes to one of the sections above:
   * ``Add mechanism to suspend providers (#30422)``

4.2.2
.....

Misc
~~~~

* ``Deprecate 'delegate_to' param in GCP operators and update docs (#29088)``

4.2.1
.....

Misc
~~~~
* ``Remove outdated compat imports/code from providers (#28507)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.2.0
.....

Features
~~~~~~~~

* ``Add _serialize_cell method to TrinoHook and PrestoHook (#27724)``

Bug Fixes
~~~~~~~~~

* ``Bump common.sql provider to 1.3.1 (#27888)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare for follow-up release for November providers (#27774)``

4.1.0
.....

This release of provider is only available for Airflow 2.3+ as explained in the
`Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/README.md#support-for-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use DbApiHook.run for DbApiHook.get_records and DbApiHook.get_first (#26944)``
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Update docs for September Provider's release (#26731)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

4.0.1
.....

Features
~~~~~~~~

* ``Add common-sql lower bound for common-sql (#25789)``

Bug Fixes
~~~~~~~~~

* ``Fix placeholders in 'TrinoHook', 'PrestoHook', 'SqliteHook' (#25939)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.0.0
.....


Breaking changes
~~~~~~~~~~~~~~~~

Deprecated ``hql`` parameter has been removed in ``get_records``, ``get_first``, ``get_pandas_df`` and ``run``
methods of the ``PrestoHook``.

Remove ``PrestoToSlackOperator`` in favor of Slack provider ``SqlToSlackOperator``.

* ``Remove 'PrestoToSlackOperator' (#25425)``

Breaking changes
~~~~~~~~~~~~~~~~

* ``Deprecate hql parameters and synchronize DBApiHook method APIs (#25299)``

Features
~~~~~~~~~

* ``Unify DbApiHook.run() method with the methods which override it (#23971)``


3.1.0
.....

Features
~~~~~~~~

* ``Adding generic 'SqlToSlackOperator' (#24663)``
* ``Move all SQL classes to common-sql provider (#24836)``

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

Features
~~~~~~~~

* ``Add 'PrestoToSlackOperator' (#23979)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate Presto example DAGs to new design #22459 (#24145)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Make presto and trino compatible with airflow 2.1 (#23061)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``

2.2.0
.....

Features
~~~~~~~~

* ``Pass X-Presto-Client-Info in presto hook (#22416)``

2.1.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.1.1
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.1.0
.....

Features
~~~~~~~~

* ``Add GCSToPrestoOperator (#21084)``
* ``Quick Update GCS Presto (#21855)``
* ``Replaced hql references to sql in TrinoHook and PrestoHook (#21630)``

Bug Fixes
~~~~~~~~~

* ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
* ``Fix K8S changelog to be PyPI-compatible (#20614)``
* ``Fix MyPy Errors for Presto provider (#20244)``

Misc
~~~~

* ``Support for Python 3.10``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
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

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Adds interactivity when generating provider documentation. (#15518)``
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``Remove Backport Providers (#14886)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.2
.....

Bug fixes
~~~~~~~~~

* ``Corrections in docs and tools after releasing provider RCs (#14082)``

1.0.1
.....

Updated documentation and readme files.

1.0.0
.....

Initial version of the provider.
