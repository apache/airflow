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

``apache-airflow-providers-celery``


Changelog
---------

3.15.0
......

Features
~~~~~~~~

* ``Add result_backend_transport_options for Redis Sentinel support in Celery (#59498)``

Misc
~~~~

* ``Cleanup of variables in settings.py (#59875)``
* ``Remove top-level SDK reference in Core (#59817)``
* ``Add and fix SIM107 and B012 Ruff rule (#59770)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``TaskInstance unused method cleanup (#59835)``
   * ``Revert "Remove PriorityWeightStrategy reference in SDK" (#59828)``
   * ``Remove PriorityWeightStrategy reference in SDK (#59780)``
   * ``Refactor/sqla2 providers(celery, kubernetes, databricks, mysql) to remove SQLA query usage (#59537)``

3.14.1
......

Misc
~~~~

* ``Add backcompat for exceptions in providers (#58727)``
* ``Move the traces and metrics code under a common observability package (#56187)``
* ``Remove global from celery provider (#58869)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.14.0
......

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Features
~~~~~~~~

* ``Send executor integration info in workload (#57800)``
* ``Add duplicate hostname check for Celery workers (#58591)``

Misc
~~~~

* ``Move out some exceptions to TaskSDK (#54505)``
* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``
* ``Fix lower bound dependency to common-compat provider (#58833)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``
   * ``Prepare release for 2025-11-27 wave of providers (#58697)``

3.13.1
......

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``
* ``Migrate 'celery' to 'common.compat' (#57322)``

Doc-only
~~~~~~~~

* ``[Doc] Fixing some typos and spelling errors (#57225)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable PT006 rule to celery Provider test (#57938)``
   * ``Enable ruff PLW1641 rule (#57679)``
   * ``Enable ruff PLW1510 rule (#57660)``
   * ``Fix code formatting via ruff preview (#57641)``

3.13.0
......

Features
~~~~~~~~

* ``Add CLI command to remove all queues from Celery worker (#56195)``

Misc
~~~~

* ``Remove CELERY_APP_NAME deprecation (#56835)``

Doc-only
~~~~~~~~

* ``Update celery broker_url config description (#56917)``
* ``Update broken link for monkey-patch reference (#56862)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix use of DeprecationWarning in celery provider to AirflowProviderDeprecationWarning (#56526)``
   * ``Remove placeholder Release Date in changelog and index files (#56056)``

3.12.4
......


Bug Fixes
~~~~~~~~~

* ``Only send hostname to celery worker if passed in cli (#55913)``
* ``Don't check db migration needlessly for 'airflow celery' cli commands. (#55878)``
* ``Fix: Use get instead of hasattr for task_result in BulkStateFetcher (#52839)``

Misc
~~~~

* ``AIP-67 - Multi Team: Pass args/kwargs to super in CeleryExecutor (#56006)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix pytest collection failure for classes decorated with context managers (#55915)``
   * ``Prepare release for Sep 2025 2nd wave of providers (#55688)``
   * ``Switch all airflow logging to structlog (#52651)``
   * ``Fix celery tests with Python 3.13 after 5.5.3 (#56017)``

3.12.3
......


Bug Fixes
~~~~~~~~~

* ``Fix setproctitle usage on macos (#53122)``
* ``Fix celery visibility timeout (set to 23.5h instead of the intended 24h) (#54480)``

Misc
~~~~

* ``Remove MappedOperator inheritance (#53696)``
* ``Fix mypy no-redef errors for timeout imports in providers (#54471)``
* ``Update usage of timeout contextmanager from SDK where possible (#54183)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove airflow.models.DAG (#54383)``
   * ``Switch pre-commit to prek (#54258)``
   * ``make bundle_name not nullable (#47592)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

3.12.2
......

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup mypy ignores in celery provider (#53261)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Use standard library ''typing'' imports for Python 3.10+ (#53158)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bumping min version of azure-storage-blob to 12.26.0 (#53440)``
   * ``Resolve OOM When Reading Large Logs in Webserver (#49470)``
   * ``Make dag_version_id in TI non-nullable (#50825)``
   * ``OpenTelemetry traces implementation cleanup (#49180)``

3.12.1
......

Bug Fixes
~~~~~~~~~

* ``Prevent legacy static hybrid executors to be running in Airflow 3 (#51760)``

Misc
~~~~

* ``Upgrade ruff to latest version (0.12.1) (#52562)``
* ``Drop support for Python 3.9 (#52072)``
* ``Use BaseSensorOperator from task sdk in providers (#52296)``
* ``Remove unused import Sequence from the celery_executor.py (#52290)``
* ``Move type-ignores up one line (#52195)``
* ``Ignore mypy errors for deprecated executors (#52187)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove unused pytestmark = pytest.mark.db_test (#52067)``

3.12.0
......

Features
~~~~~~~~

* ``Enhance Celery CLI with Worker and Queue Management Features (#51257)``

Bug Fixes
~~~~~~~~~

* ``Fix Celery executor subprocess to stream stdout/stderr using subprocess.run (#50682)``

Misc
~~~~

* ``Add worker_umask to celery provider.yaml (#51218)``
* ``Remove Airflow 2 code path in executors (#51009)``
* ``Prevent legacy static hybrid executors to be running in Airflow 3 (#51733)``

Doc-only
~~~~~~~~

* ``Update the executor and provider doc to highlight the two statically coded hybrid executors are no longer supported in Airflow 3.0.0+ (#51715)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.11.0
......

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Bug Fixes
~~~~~~~~~

* ``Fix execution API server URL handling for relative paths (#49782)``

Misc
~~~~

* ``Min provider version=2.10; use running_state freely (#49924)``
* ``Remove AIRFLOW_2_10_PLUS conditions (#49877)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``
* ``Make default execution server URL be relative to API Base URL (#49747)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Revert "Limit Celery to not include 5.5.2 (#49940)" (#49951)``
   * ``Limit Celery to not include 5.5.2 (#49940)``
   * ``Avoid committing history for providers (#49907)``
   * ``capitalize the term airflow (#49450)``
   * ``Prepare docs for Apr 3rd wave of providers (#49338)``
   * ``Move celery integration tests to celery provider. (#49178)``

3.10.6
......

Bug Fixes
~~~~~~~~~

* ``Bring back serve_logs to be in the core (#49031)``

Misc
~~~~

* ``Remove fab from preinstalled providers (#48457)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Improve documentation building iteration (#48760)``

3.10.5
......

Bug Fixes
~~~~~~~~~

* ``Fix Celery Executor on Airflow 2.x again. (#48806)``

Misc
~~~~

* ``Remove change_sensor_mode_to_reschedule from base executor (#48649)``
* ``Update min version of Celery library to 5.5.0 (#43777)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Simplify tooling by switching completely to uv (#48223)``

3.10.4
......

Bug Fixes
~~~~~~~~~

* ``Scheduler shouldn't crash when 'executor_config' is passed for executors using task SDK (#47548)``

Misc
~~~~

 * ``AIP-81: Flatten core CLI commands (#48224)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Bump various providers in preparation for Airflow 3.0.0b4 (#48013)``
   * ``Remove links to x/twitter.com (#47801)``

3.10.3
......

Bug Fixes
~~~~~~~~~

* ``Avoid scheduler crash with passing executor_config with CeleryExecutor (#47375)``
* ``bugfix: cannot import name 'workloads' for Airflow v2 (#47152)``

Misc
~~~~

* ``Get rid of google-re2 as dependency (#47493)``
* ``Remove the old 'task run' commands and LocalTaskJob (#47453)``
* ``Disable ORM access from Tasks, DAG processing and Triggers (#47320)``
* ``Implement stale dag bundle cleanup (#46503)``
* ``Render structured logs in the new UI rather than showing raw JSON (#46827)``
* ``Move execution_api_server_url config to the core section (#46969)``
* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

3.10.2
......

.. note::
  This version has no code changes. It's released due to yank of previous version due to packaging issues.

3.10.1
......

Bug Fixes
~~~~~~~~~

* ``Fixing log typos in Local & Celery Executors (#46866)``

Misc
~~~~

* ``Rework the TriggererJobRunner to run triggers in a process without DB access (#46677)``
* ``AIP-66: Make DAG callbacks bundle aware (#45860)``
* ``Swap CeleryExecutor over to use TaskSDK for execution. (#46265)``
* ``Remove 2.8 version check from CeleryExecutor CLI (#46910)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``

3.10.0
......

Features
~~~~~~~~

* ``Add support for custom celery configs (#45038)``

Bug Fixes
~~~~~~~~~

* ``Fix Version Check for CLI Imports in Celery provider (#45255)``

Misc
~~~~

* ``AIP-72: Support DAG parsing context in Task SDK (#45694)``
* ``AIP-72: Support better type-hinting for Context dict in SDK  (#45583)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``move Celery provider to new provider code structure (#45786)``
   * ``Move new provider tests to "provider_tests" submodule (#45955)``
   * ``Add script to move providers to the new directory structure (#45945)``
   * ``move standard, alibaba and common.sql provider to the new structure (#45964)``
   * ``Prepare docs for ad hoc release celery provider Jan 2025 (#45942)``

3.9.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``AIP-81 Move CLI Commands to directories according to Hybrid, Local and Remote (#44538)``
* ``Remove AIP-44 configuration from the code (#44454)``

3.8.5
.....

Bug Fixes
~~~~~~~~~

* ``Re-queue tassk when they are stuck in queued (#43520)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``

3.8.4
.....

Misc
~~~~

* ``AIP-72: Remove DAG pickling (#43667)``
* ``Move python operator to Standard provider (#42081)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

3.8.3
.....

Bug Fixes
~~~~~~~~~

* ``All executors should inherit from BaseExecutor (#41904)``
* ``Remove state sync during celery task processing (#41870)``

Misc
~~~~

* ``Change imports to use Standard provider for BashOperator (#42252)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.8.2
.....

Misc
~~~~

* ``remove deprecated soft_fail from providers (#41710)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.8.1
.....

Bug Fixes
~~~~~~~~~

* ``fix: Missing 'slots_occupied' in 'CeleryKubernetesExecutor' and 'LocalKubernetesExecutor' (#41602)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.8.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``
* ``Remove deprecated SubDags (#41390)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.7.3
.....

Bug Fixes
~~~~~~~~~

* ``Increase broker's visibility timeout to 24hrs (#40879)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave July 2024 (#40644)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

3.7.2
.....

Bug Fixes
~~~~~~~~~

* ``Fixing exception types to include TypeError, which is what is raised in (#40012)``
* ``catch sentry flush if exception happens in _execute_in_fork finally block (#40060)``

Misc
~~~~

* ``Add PID and return code to _execute_in_fork logging (#40058)``

3.7.1
.....

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``
* ``ECS Executor: Set tasks to RUNNING state once active (#39212)``
* ``Remove compat code for 2.7.0 - its now the min Airflow version (#39591)``
* ``misc: add comment about remove unused code (#39748)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

3.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

3.6.2
.....

Bug Fixes
~~~~~~~~~

* ``Ensure __exit__ is called in decorator context managers (#38383)``
* ``Don't dispose sqlalchemy engine when using internal api (#38562)``
* ``Use celery worker CLI from Airflow package for Airflow < 2.8.0 (#38879)``

Misc
~~~~

* ``Allow to use 'redis'>=5 (#38385)``
* ``Reraise of AirflowOptionalProviderFeatureException should be direct (#38555)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump ruff to 0.3.3 (#38240)``

3.6.1
.....

Bug Fixes
~~~~~~~~~

* ``Remove pid arg from celery option to fix duplicate pid issue, Move celery command to provider package (#36794)``
* ``Change AirflowTaskTimeout to inherit BaseException (#35653)``

Misc
~~~~

* ``Migrate executor docs to respective providers (#37728)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Resolve G003: "Logging statement uses +" (#37848)``
   * ``Add comment about versions updated by release manager (#37488)``

3.6.0
.....

Features
~~~~~~~~

* ``Add 'task_acks_late' configuration to Celery Executor (#37066)``

Misc
~~~~

* ``improve info for prevent celery command autoscale misconfig (#36576)``

3.5.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix stacklevel in warnings.warn into the providers (#36831)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Standardize airflow build process and switch to Hatchling build backend (#36537)``
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

3.5.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix 'sentinel_kwargs' load from ENV (#36318)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.5.0
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
   * ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
   * ``Update information about links into the provider.yaml files (#35837)``
   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for providers (#35693)``
   * ``Prepare docs 1st wave of Providers November 2023 (#35537)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``

3.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix _SECRET and _CMD broker configuration (#34782)``
* ``Remove sensitive information from Celery executor warning (#34954)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D401 Support - A thru Common (Inclusive) (#34934)``


3.4.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``respect soft_fail argument when exception is raised for celery sensors (#34474)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor usage of str() in providers (#34320)``

3.3.4
.....

Bug Fixes
~~~~~~~~~

* ``Fix condition of update_task_state in celery executor (#34192)``

Misc
~~~~

* ``Combine similar if logics in providers (#33987)``
* ``Limit celery by excluding 5.3.2 and 5.3.3 (#34031)``
* ``Replace try - except pass by contextlib.suppress in providers (#33980)``
* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``

3.3.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix dependencies for celery and opentelemetry for Python 3.8 (#33579)``

Misc
~~~~~

* ``Make auth managers provide their own airflow CLI commands (#33481)``
* ``Refactor Sqlalchemy queries to 2.0 style (Part 7) (#32883)``

3.3.2
.....

Misc
~~~~
* ``Add missing re2 dependency to cncf.kubernetes and celery providers (#33237)``
* ``Replace State by TaskInstanceState in Airflow executors (#32627)``

3.3.1
.....

Misc
~~~~

* ``aDd documentation generation for CLI commands from executors (#33081)``
* ``Get rid of Python2 numeric relics (#33050)``

3.3.0
.....

.. note::
  This provider release is the first release that has Celery Executor and
  Celery Kubernetes Executor moved from the core ``apache-airflow`` package to a Celery
  provider package. It also expects ``apache-airflow-providers-cncf-kubernetes`` in version 7.4.0+ installed
  in order to use ``CeleryKubernetesExecutor``. You can install the provider with ``cncf.kubernetes`` extra
  with ``pip install apache-airflow-providers-celery[cncf.kubernetes]`` to get the right version of the
  ``cncf.kubernetes`` provider installed.

Features
~~~~~~~~

* ``Move CeleryExecutor to the celery provider (#32526)``
* ``Add pre-Airflow-2-7 hardcoded defaults for config for older providers  (#32775)``
* ``[AIP-51] Executors vending CLI commands (#29055)``

Misc
~~~~

* ``Move all k8S classes to cncf.kubernetes provider (#32767)``
* ``Add Executors discovery and documentation (#32532)``
* ``Move default_celery.py to inside the provider (#32628)``
* ``Raise original import error in CLI vending of executors (#32931)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Introduce decorator to load providers configuration (#32765)``
   * ``Allow configuration to be contributed by providers (#32604)``
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``D205 Support - Providers: Apache to Common (inclusive) (#32226)``
   * ``Improve provider documentation and README structure (#32125)``

3.2.1
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``Add note about dropping Python 3.7 for providers (#32015)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers (#30917)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Add mechanism to suspend providers (#30422)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

3.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add documentation for July 2022 Provider's release (#25030)``
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Update docs for September Provider's release (#26731)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
   * ``Prepare docs for new providers release (August 2022) (#25618)``
   * ``Move provider dependencies to inside provider folders (#24672)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.1.4
.....

Misc
~~~~

* ``Update our approach for executor-bound dependencies (#22573)``

2.1.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.1.2
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.1.1
.....

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Update documentation for provider December 2021 release (#20523)``
   * ``Use typed Context EVERYWHERE (#20565)``

2.1.0
.....

Features
~~~~~~~~

* ``The celery provider is converted to work with Celery 5 following airflow 2.2.0 change of Celery version``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

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
   * ``Update documentation for broken package releases (#14734)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.1
.....

Updated documentation and readme files.

1.0.0
.....

Initial version of the provider.
