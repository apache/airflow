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

``apache-airflow-providers-yandex``


Changelog
---------

3.12.0
......

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``providers/yandex: fix typing (#40997)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Aug 1st wave of providers (#41230)``
   * ``Prepare docs 1st wave July 2024 (#40644)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

3.11.2
......

Bug Fixes
~~~~~~~~~

* ``Exclude yandex versions 0.289.0, 0.290.0 (#39974)``

Misc
~~~~

* ``Fix typos in Providers docs and Yandex hook (#40277)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Limit yandex provider to avoid mypy errors (#39990)``
   * ``Workaround new yandexcloud breaking dataproc integration (#39964)``

3.11.1
......

Misc
~~~~

* `` AIP-21: yandexcloud: rename files, emit deprecation warning (#39618)``
* ``yandex provider: bump version for yq http client package (#39548)``
* ``Faster 'airflow_version' imports (#39552)``
* ``add doc about Yandex Query operator (#39445)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

3.11.0
......

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

3.10.0
......

Features
~~~~~~~~

* ``Add Yandex Query support from Yandex.Cloud (#37458)``

Misc
~~~~

* ``support iam token from metadata, simplify code (#38411)``
* ``Avoid use of 'assert' outside of the tests (#37718)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave (RC1) April 2024 (#38863)``
   * ``docs: yandex provider grammatical improvements (#38589)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Prepare docs 1st wave (RC1) March 2024 (#37876)``
   * ``Add comment about versions updated by release manager (#37488)``

3.9.0
.....

Features
~~~~~~~~

* ``Add secrets-backends section into the Yandex provider yaml definition (#37065)``

Bug Fixes
~~~~~~~~~

* ``fix: using endpoint from connection if not specified (#37076)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D401 Support in Providers (simple) (#37258)``
   * ``docs: update description in airflow provider.yaml (#37096)``

3.8.0
.....

Features
~~~~~~~~

* ``feat: add Yandex Cloud Lockbox secrets backend (#36449)``


Bug Fixes
~~~~~~~~~

* ``Fix stacklevel in warnings.warn into the providers (#36831)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Provide the logger_name param in providers hooks in order to override the logger name (#36675)``
   * ``Revert "Provide the logger_name param in providers hooks in order to override the logger name (#36675)" (#37015)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

3.7.1
.....

Bug Fixes
~~~~~~~~~

* ``Follow BaseHook connection fields method signature in child classes (#36086)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix and reapply templates for provider documentation (#35686)``
   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for provider packages (#35693)``

3.6.0
.....

Features
~~~~~~~~

* ``Yandex dataproc deduce default service account (#35059)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``

3.5.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``

3.4.0
.....

.. note::
  This release dropped support for Python 3.7

Features
~~~~~~~~

* ``add support for Yandex Dataproc cluster labels (#29811)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add note about dropping Python 3.7 for providers (#32015)``
   * ``Add D400 pydocstyle check - Providers (#31427)``
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Add mechanism to suspend providers (#30422)``
   * ``Resume yandex provider (#33574)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Improve provider documentation and README structure (#32125)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
   * ``Bump minimum Airflow version in providers (#30917)``
   * ``Suspend Yandex provider due to protobuf limitation (#30667)``

3.3.0
.....

Features
~~~~~~~~

* ``support Yandex SDK feature "endpoint" (#29635)``

3.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* In YandexCloudBaseHook, non-prefixed extra fields are supported and are preferred (#27040).  E.g. ``folder_id`` will be preferred if ``extra__yandexcloud__folder_id`` is also present.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Update docs for September Provider's release (#26731)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
   * ``pRepare docs for November 2022 wave of Providers (#27613)``
   * ``Prepare for follow-up release for November providers (#27774)``

3.1.0
.....

Features
~~~~~~~~

* ``YandexCloud provider: Support new Yandex SDK features for DataProc (#25158)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add documentation for July 2022 Provider's release (#25030)``
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate Yandex example DAGs to new design AIP-47 (#24082)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.2.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.2.2
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.2.1
.....

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Add optional features in providers. (#21074)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Fix spelling (#22054)``

2.2.0
.....

Features
~~~~~~~~

* ``YandexCloud provider: Support new Yandex SDK features: log_group_id, user-agent, maven packages (#20103)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix mypy for providers: elasticsearch, oracle, yandex (#20344)``
   * ``Fixup string concatenations (#19099)``
   * ``Update documentation for November 2021 provider's release (#19882)``
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``Update documentation for September providers release (#18613)``
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
   * ``Inclusive Language (#18349)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.1.0
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``


Features
~~~~~~~~

* ``Add autoscaling subcluster support and remove defaults (#17033)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Prepares docs for Rc2 release of July providers (#17116)``
   * ``Remove/refactor default_args pattern for miscellaneous providers (#16872)``
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
   * ``Update docstrings to adhere to sphinx standards (#14918)``
   * ``Remove Backport Providers (#14886)``
   * ``Update documentation for broken package releases (#14734)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Fix Sphinx Issues with Docstrings (#14968)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.1
.....

Updated documentation and readme files.

1.0.0
.....

Initial version of the provider.
