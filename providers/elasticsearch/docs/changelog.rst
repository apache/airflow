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

6.4.1
.....

Misc
~~~~

* ``Add backcompat for exceptions in providers (#58727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.4.0
.....

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``

6.3.5
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable ruff PLW2101,PLW2901,PLW3301 rule (#57700)``
   * ``Enable PT006 rule to 19 files in providers (airbyte, alibaba, atlassian, papermill, presto, redis, singularity, sqlite, tableau, vertica, weaviate, elasticsearch, exasol) (#57986)``
   * ``Enable ruff PLW0120 rule (#57456)``

6.3.4
.....

Misc
~~~~

* ``fix mypy type errors in elasticsearch provider for sqlalchemy 2 upgrade (#56818)``
* ``Migrate Apache providers & Elasticsearch to ''common.compat'' (#57016)``

Doc-only
~~~~~~~~

* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable PT011 rule to prvoider tests (#56277)``

6.3.3
.....


Bug Fixes
~~~~~~~~~

* ``[OSSTaskHandler, CloudwatchTaskHandler, S3TaskHandler, HdfsTaskHandler, ElasticsearchTaskHandler, GCSTaskHandler, OpensearchTaskHandler, RedisTaskHandler, WasbTaskHandler] supports log file size handling (#55455)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare release for Sep 2025 1st wave of providers (#55203)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``
   * ``Remove airflow.models.DAG (#54383)``
   * ``Switch pre-commit to prek (#54258)``

6.3.2
.....

Bug Fixes
~~~~~~~~~

* ``Make Elasticsearch/OpensearchTaskHandler to render log well (#53639)``
* ``Resolve OOM When Reading Large Logs in Webserver (#49470)``

Misc
~~~~

* ``fix mypy unreachable code warnings for elasticsearch provider (#53464)``
* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup mypy ignore in elasticsearch provider where possible (#53277)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Make elasticsearch provider compatible with mypy 1.16.1 (#53109)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Move all BaseHook usages to version_compat in Elastic Search (#52805)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Deprecate decorators from Core (#53629)``

6.3.1
.....

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Provider Migration: Update elasticsearch for Airflow 3.0 compatibility (#52628)``
* ``Disable UP038 ruff rule and revert mandatory 'X | Y' in insintance checks (#52644)``
* ``Drop support for Python 3.9 (#52072)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Removed pytestmark db_test from the elasticsearch providers tests (#52139)``

6.3.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Migrate 'ElasticsearchSQLHook' to use 'get_df' (#50454)``
* ``Remove AIRFLOW_2_10_PLUS conditions (#49877)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``
   * ``test: migrate 'get_pandas_df' to 'get_df' in 'provider' test (#49339)``
   * ``capitalize the term airflow (#49450)``

6.2.2
.....

Bug Fixes
~~~~~~~~~

* ``Ignore cursor specific parameters when instantiating the connection (#48865)``

Misc
~~~~

* ``remove superfluous else block (#49199)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Apr 2nd wave of providers (#49051)``
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``
   * ``Prepare docs for Apr 1st wave of providers (#48828)``
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Prepare docs for Mar 2nd wave of providers (#48383)``
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``

6.2.1
.....

Misc
~~~~

* ``Render structured logs in the new UI rather than showing raw JSON (#46827)``
* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

6.2.0
.....

.. note::
  This version has no code changes. It's released due to yank of previous version due to packaging issues.


6.1.0
.....

Features
~~~~~~~~

* ``Implemented cursor for ElasticsearchSQLHook so it can be used through SQLExecuteQueryOperator (#46439)``
* ``Add write feature to ESTaskHandler (#44973)``

Misc
~~~~

* ``Start porting mapped task to SDK (#45627)``
* ``Update index.rst (#45263)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Fix doc issues found with recent moves (#46372)``
   * ``refactor(providers/elasticsearch): move elasticsearch provider to new structure (#46146)``

6.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
   All deprecated classes, parameters and features have been removed from the ElasticSearch provider package.
   The following breaking changes were introduced:

   * Hooks
      * Remove ``airflow.providers.elasticsearch.hooks.elasticsearch.ElasticsearchHook``. Use ``airflow.providers.elasticsearch.hooks.elasticsearch.ElasticsearchSQLHook`` instead.
   * Log
      * Removed ``log_id_template`` parameter from ``ElasticsearchTaskHandler``.
      * Removed ``retry_timeout`` parameter from ``ElasticsearchTaskHandler``. Use ``retry_on_timeout`` instead

* ``Remove Provider Deprecations in Elasticsearch (#44629)``

Misc
~~~~

* ``Remove references to AIRFLOW_V_2_9_PLUS (#44987)``
* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Consistent way of checking Airflow version in providers (#44686)``
* ``Update DAG example links in multiple providers documents (#44034)``
* ``Rename execution_date to logical_date across codebase (#43902)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Update path of example dags in docs (#45069)``

5.5.3
.....

Misc
~~~~

* ``Add support for semicolon stripping to DbApiHook, PrestoHook, and TrinoHook (#41916)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

5.5.2
.....

Misc
~~~~

* ``Removed conditional check for task context logging in airflow version 2.8.0 and above (#42764)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix spelling; 'Airlfow' -> 'Airflow' (#42855)``

5.5.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix ElasticSearch SQLClient deprecation warning (#41871)``

Misc
~~~~

* ``Generalize caching of connection in DbApiHook to improve performance (#40751)``
* ``filename template arg in providers file task handlers backward compatibility support (#41633)``
* ``Remove deprecated log handler argument filename_template (#41552)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

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
   * ``Use reproducible builds for providers (#35693)``

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

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
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

.. warning:: This release has been **yanked** with a reason: ``Elasticsearch provider is incompatible with Airflow <2.3``

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

3.0.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``Elasticsearch provider is incompatible with Airflow <2.3``

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
