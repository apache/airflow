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

``apache-airflow-providers-elasticsearch``


Changelog
---------

5.5.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Fix 'ElasticsearchSQLHook' fails with 'AttributeError: __enter__' (#41537)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.4.2
.....

Misc
~~~~

* ``Clean up remaining getattr connection DbApiHook (#40665)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave July 2024 (#40644)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

5.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Make elastic search index_pattern more configurable (#38423)``

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``
* ``Scheduler to handle incrementing of try_number (#39336)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

5.4.0
.....
.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

5.3.4
.....

Misc
~~~~

* ``Add default for 'task' on TaskInstance / fix attrs on TaskInstancePydantic (#37854)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update yanked versions in providers changelogs (#38262)``
   * ``Prepare docs 1st wave (RC1) March 2024 (#37876)``
   * ``Add comment about versions updated by release manager (#37488)``
   * ``Fix D105 checks for ES provider (#37880)``

5.3.3
.....

Misc
~~~~

* ``Avoid 'pendulum.from_timestamp' usage (#37160)``
* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``

5.3.2
.....

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

5.3.1
.....

Misc
~~~~

* ``Remove getattr in es task handler when airflow min version bumped to 2.6 (#36431)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Re-apply updated version numbers to 2nd wave of providers in December (#36380)``

5.3.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``
* ``Cleanup code for elasticsearch<8 (#35707)``

5.2.0
.....

Features
~~~~~~~~

* ``Add task context logging feature to allow forwarding messages to task logs (#32646)``
* ``Extend task context logging support for remote logging using Elasticsearch (#32977)``

Bug Fixes
~~~~~~~~~

* ``Update es read query to not use body (#34792)``
* ``Check attr on parent not self re TaskContextLogger set_context (#35780)``

Misc
~~~~

* ``Remove backcompat inheritance for DbApiHook (#35754)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix and reapply templates for provider documentation (#35686)``
   * ``Use reproducible builds for provider packages (#35693)``

5.1.1
.....

Misc
~~~~

* ``Use None instead of empty data structures when no ElasticSearch logs (#34793)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``D401 Support - Providers: DaskExecutor to Github (Inclusive) (#34935)``

5.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

.. note::
  In PR #34790 we removed the unused argument ``metadata`` from method ``es_read``.  We determined that ``es_read``
  is an internal method and therefore not subject to backcompat, so we did not bump major version for this provider.
  In order to make clearer that this is an internal method we renamed it with an underscore prefix ``_es_read``.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Remove unused argument metadata from es_read and make clearly private (#34790)``
* ``Improve intelligibility of end_of_log determination (#34788)``
* ``Replace try/except/pass by contextlib.suppress in ElasticSearch provider (#34251)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor: consolidate import time in providers (#34402)``
   * ``Refactor shorter defaults in providers (#34347)``


5.0.2
.....

Bug Fixes
~~~~~~~~~

* ``Make sure that only valid elasticsearch keys are passed to handler (#34119)``

Misc
~~~~

* ``Replace sequence concatenation by unpacking in Airflow providers (#33933)``
* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``
* ``Use literal dict instead of calling dict() in providers (#33761)``
* ``remove unnecessary and rewrite it using list in providers (#33763)``
* ``Use f-string instead of  in Airflow providers (#33752)``

5.0.1
.....

.. note::
  This release added support for elasticsearch 8

Bug Fixes
~~~~~~~~~

* ``Add backward compatibility for elasticsearch<8 (#33281)``
* ``Fix urlparse schemaless-behaviour on Python 3.9+ (#33289)``

Misc
~~~~

* ``Upgrade Elasticsearch to 8 (#33135)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Replace strftime with f-strings where nicer (#33455)``
   * ``D205 Support - Providers - Final Pass (#33303)``

5.0.0
.....

.. note::
  Upgrade to Elasticsearch 8. The ElasticsearchTaskHandler & ElasticsearchSQLHook will now use Elasticsearch 8 package.
  As explained https://elasticsearch-py.readthedocs.io/en/stable , Elasticsearch language clients are only backwards
  compatible with default distributions and without guarantees made, we recommend upgrading the version of
  Elasticsearch database to 8 to ensure compatibility with the language client.

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  Deprecate non-official elasticsearch libraries. Only the official elasticsearch library was used

* ``Deprecate the 2 non-official elasticsearch libraries (#31920)``

Bug Fixes
~~~~~~~~~

* ``Fix unsound type hint in ElasticsearchTaskHandler.es_read (#32509)``

Misc
~~~~

* ``Fix Failing ES Remote Logging (#32438)``

.. Review and move the new changes to one of the sections above:
   * ``D205 Support - Providers: Databricks to Github (inclusive) (#32243)``
   * ``Improve provider documentation and README structure (#32125)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``Add deprecation info to the providers modules and classes docstring (#32536)``
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``

4.5.1
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``Remove Python 3.7 support (#30963)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Improve docstrings in providers (#31681)``
   * ``Add D400 pydocstyle check - Providers (#31427)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``

4.5.0
.....

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers (#30917)``
* ``Upper-bind elasticearch integration (#31255)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
   * ``Restore trigger logging (#29482)``
   * ``Revert "Enable individual trigger logging (#27758)" (#29472)``
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Add mechanism to suspend providers (#30422)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

4.4.0
.....

Features
~~~~~~~~

* ``Enable individual trigger logging (#27758)``

4.3.3
.....

Bug Fixes
~~~~~~~~~

* ``Allow nested attr in elasticsearch host_field (#28878)``

4.3.2
.....

Bug Fixes
~~~~~~~~~

* ``Support restricted index patterns in Elasticsearch log handler (#23888)``

4.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Bump common.sql provider to 1.3.1 (#27888)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare for follow-up release for November providers (#27774)``

4.3.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``

4.2.1
.....

Misc
~~~~

* ``Add common-sql lower bound for common-sql (#25789)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

4.2.0
.....

Features
~~~~~~~~

* ``Improve ElasticsearchTaskHandler (#21942)``


4.1.0
.....

Features
~~~~~~~~

* ``Adding ElasticserachPythonHook - ES Hook With The Python Client (#24895)``
* ``Move all SQL classes to common-sql provider (#24836)``

Bug Fixes
~~~~~~~~~

* ``Move fallible ti.task.dag assignment back inside try/except block (#24533) (#24592)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Only assert stuff for mypy when type checking (#24937)``
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

4.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Apply per-run log templates to log handlers (#24153)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix new MyPy errors in main (#22884)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``removed old files (#24172)``
   * ``Prepare provider documentation 2022.05.11 (#23631)``
   * ``Use new Breese for building, pulling and verifying the images. (#23104)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

3.0.3
.....

Bug Fixes
~~~~~~~~~

* ``Make ElasticSearch Provider compatible for Airflow<2.3 (#22814)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update black precommit (#22521)``

3.0.2 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Elasticsearch 3.0.2 is incompatible with Airflow >2.3``

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``
* ``Fix "run_id" k8s and elasticsearch compatibility with Airflow 2.1 (#22385)``

3.0.1 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Elasticsearch provider is incompatible with Airlfow <2.3``

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

3.0.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Elasticsearch provider is incompatible with Airlfow <2.3``

Breaking changes
~~~~~~~~~~~~~~~~

* ``Change default log filename template to include map_index (#21495)``


Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Type TaskInstance.task to Operator and call unmap() when needed (#21563)``

2.2.0
.....

Features
~~~~~~~~

* ``Emit "logs not found" message when ES logs appear to be missing (#21261)``
* ``Use compat data interval shim in log handlers (#21289)``

Misc
~~~~

* ``Clarify ElasticsearchTaskHandler docstring (#21255)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Fix mypy for providers: elasticsearch, oracle, yandex (#20344)``
   * ``Fix duplicate changelog entries (#19759)``
   * ``Add pre-commit check for docstring param types (#21398)``
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Update documentation for provider December 2021 release (#20523)``
   * ``Update documentation for November 2021 provider's release (#19882)``

2.1.0
.....

Features
~~~~~~~~

* ``Add docs for AIP 39: Timetables (#17552)``
* ``Adds example showing the ES_hook (#17944)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update documentation for September providers release (#18613)``
   * ``Updating the Elasticsearch example DAG to use the TaskFlow API (#18565)``

2.0.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix Invalid log order in ElasticsearchTaskHandler (#17551)``

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``
* ``Adds secrets backend/logging/auth information to provider yaml (#17625)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``

2.0.2
.....

Bug Fixes
~~~~~~~~~

* Updated dependencies to allow Python 3.9 support

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.0.1
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``
* ``Remove support Jinja templated log_id in Elasticsearch (#16465)``

  While undocumented, previously ``[elasticsearch] log_id`` supported a Jinja templated string.
  Support for Jinja templates has now been removed. ``log_id`` should be a template string instead,
  for example: ``{dag_id}-{task_id}-{execution_date}-{try_number}``.

  If you used a Jinja template previously, the ``execution_date`` on your Elasticsearch documents will need
  to be updated to the new format.

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

Features
~~~~~~~~

* ``Support remote logging in elasticsearch with filebeat 7 (#14625)``
* ``Support non-https elasticsearch external links (#16489)``

Bug fixes
~~~~~~~~~

* ``Fix external elasticsearch logs link (#16357)``
* ``Fix Elasticsearch external log link with ''json_format'' (#16467)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Docs: Fix url for ''Elasticsearch'' (#16275)``
   * ``Add ElasticSearch Connection Doc (#16436)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

2.0.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Released by Mistake!``

1.0.4
.....

Bug fixes
~~~~~~~~~

* ``Fix 'logging.exception' redundancy (#14823)``
* ``Fix exception caused by missing keys in the ElasticSearch Record (#15163)``

1.0.3
.....

Bug fixes
~~~~~~~~~

* ``Elasticsearch Provider: Fix logs downloading for tasks (#14686)``

1.0.2
.....

Bug fixes
~~~~~~~~~

* ``Corrections in docs and tools after releasing provider RCs (#14082)``

1.0.1
.....

Updated documentation and readme files.

Bug fixes
~~~~~~~~~

* ``Respect LogFormat when using ES logging with Json Format (#13310)``


1.0.0
.....

Initial version of the provider.
