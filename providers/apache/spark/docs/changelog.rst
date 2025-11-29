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


``apache-airflow-providers-apache-spark``



Changelog
---------

5.4.0
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

5.3.4
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable ruff PLW2101,PLW2901,PLW3301 rule (#57700)``
   * ``Enable PT006 rule to 13 files in providers (apache) (#57998)``
   * ``Enable ruff PLW0129 rule (#57516)``
   * ``Enable PT011 rule to provider tests (#57528)``

5.3.3
.....

Misc
~~~~

* ``Use ValueError instead of RuntimeError when resolving SparkSubmitHook connection (#56631)``
* ``Migrate Apache providers & Elasticsearch to ''common.compat'' (#57016)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare release for Sep 2025 2nd wave of providers (#55688)``
   * ``Prepare release for Sep 2025 1st wave of providers (#55203)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``
   * ``Remove airflow.models.DAG (#54383)``
   * ``Make term Dag consistent in providers docs (#55101)``
   * ``Replace API server's direct Connection access workaround in BaseHook (#54083)``
   * ``Switch pre-commit to prek (#54258)``
   * ``make bundle_name not nullable (#47592)``
   * ``Enable PT011 rule to provider tests (#56578)``
   * ``Remove placeholder Release Date in changelog and index files (#56056)``

5.3.2
.....

Misc
~~~~

* ``Deprecate decorators from Core (#53629)``
* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup type ignores (#53301)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Replace BaseHook to Task SDK for apache/pyspark (#52842)``
* ``Replace 'BaseHook' to Task SDK for 'apache/spark' (#52683)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Make dag_version_id in TI non-nullable (#50825)``

5.3.1
.....

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``add: version_compat (#52448)``
* ``Drop support for Python 3.9 (#52072)``
* ``Replace 'models.BaseOperator' to Task SDK one for Standard Provider (#52292)``

Doc-only
~~~~~~~~

* ``Cleanup unused args example_pyspark.py (#52492)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Separate out creation of default Connections for tests and non-tests (#52129)``
   * ``Removed pytestmark db_test from the spark provider (#52081)``
   * ``Introducing fixture to create 'Connections' without DB in provider tests (#51930)``

5.3.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Features
~~~~~~~~

* ``add root parent information to OpenLineage events (#49237)``

Misc
~~~~

* ``Bump Pyspark to even higher version (#50308)``
* ``Lower bind pyspark and pydruid to relatively new versions (#50205)``
* ``Remove AIRFLOW_2_10_PLUS conditions (#49877)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``

5.2.1
.....

Misc
~~~~

* ``remove superfluous else block (#49199)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.2.0
.....

Features
~~~~~~~~

* ``Add openlineage as Extra dep for Spark provider (#48972)``

Misc
~~~~

* ``Make '@task' import from airflow.sdk (#48896)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``
   * ``Prepare docs for Apr 1st wave of providers (#48828)``
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Upgrade ruff to latest version (#48553)``

5.1.1
.....

Features
~~~~~~~~

* ``add OpenLineage configuration injection to SparkSubmitOperator (#47508)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``

5.0.1
.....

Bug Fixes
~~~~~~~~~

* ``spark on kubernetes removes dependency on Spark Exit code (#46817)``

Misc
~~~~

* ``Upgrade flit to 3.11.0 (#46938)``

Doc-only
~~~~~~~~

* ``Include driver classpath in --jars cmd docstring in spark-submit hook and operator (#45210)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``
   * ``Prepare docs for Feb 1st wave of providers (#46893)``
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Fix doc issues found with recent moves (#46372)``
   * ``Add run_after column to DagRun model (#45732)``
   * ``Move Apache Spark to new provider structure (#46108)``

5.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  All deprecated classes, parameters and features have been removed from the Apache Spark provider package.
  The following breaking changes were introduced:

  * Operators

    * Removed ``_sql()`` support for SparkSqlOperator. Please use ``sql`` attribute instead. ``_sql`` was
      introduced in 2016 and since it was listed as templated field, which is no longer the case, we
      handled it as public api despite the ``_`` prefix that marked it as private.

* ``Remove deprecated code from apache spark provider (#44567)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Fix failing mypy check on 'main' (#44191)``
* ``spark-submit: replace 'principle' by 'principal' (#44150)``
* ``Update DAG example links in multiple providers documents (#44034)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Update path of example dags in docs (#45069)``

4.11.3
......

Misc
~~~~

* ``Move python operator to Standard provider (#42081)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.11.2
......

Bug Fixes
~~~~~~~~~

* ``Changed conf property from str to dict in SparkSqlOperator (#42835)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

4.11.1
......

Misc
~~~~

* ``Refactor function resolve_kerberos_principal (#42777)``

4.11.0
......

Features
~~~~~~~~

* ``Add kerberos related connection fields(principal, keytab) on SparkSubmitHook (#40757)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.10.0
......

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``
* ``Resolve 'AirflowProviderDeprecationWarning' in 'SparkSqlOperator' (#41358)``

4.9.0
.....

Features
~~~~~~~~

* ``Add 'kubernetes_application_id' to 'SparkSubmitHook' (#40753)``

Bug Fixes
~~~~~~~~~

* ``(fix): spark submit pod name with driver as part of its name(#40732)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave July 2024 (#40644)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

4.8.2
.....

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``

4.8.1
.....

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

4.8.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Rename SparkSubmitOperator argument queue as yarn_queue (#38852)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

4.7.2
.....

Misc
~~~~

* ``Rename 'SparkSubmitOperator' fields names to comply with templated fields validation (#38051)``
* ``Rename 'SparkSqlOperator' fields name to comply with templated fields validation (#38045)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave (RC1) March 2024 (#37876)``
   * ``Add comment about versions updated by release manager (#37488)``
   * ``D401 Support in Providers (simple) (#37258)``

4.7.1
.....

Misc
~~~~

* ``Bump min version for grpcio-status in spark provider (#36662)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Revert "Provide the logger_name param in providers hooks in order to override the logger name (#36675)" (#37015)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``
   * ``Standardize airflow build process and switch to Hatchling build backend (#36537)``
   * ``Provide the logger_name param in providers hooks in order to override the logger name (#36675)``

4.7.0
.....

* ``change spark connection form and add spark connections docs (#36419)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``

4.6.0
.....

Features
~~~~~~~~

* ``SparkSubmit: Adding propertyfiles option (#36164)``
* ``SparkSubmit Connection Extras can be overridden (#36151)``

Bug Fixes
~~~~~~~~~

* ``Follow BaseHook connection fields method signature in child classes (#36086)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.5.0
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
   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for providers (#35693)``

4.4.0
.....

Features
~~~~~~~~

* ``Add pyspark decorator (#35247)``
* ``Add use_krb5ccache option to SparkSubmitOperator (#35331)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add information about Qubole removal and make it possible to release it (#35492)``


4.3.0
.....

Features
~~~~~~~~

* ``Add 'use_krb5ccache' option to 'SparkSubmitHook' (#34386)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Upgrade pre-commits (#35033)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``

4.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor usage of str() in providers (#34320)``

4.1.5
.....

Misc
~~~~

* ``Refactor regex in providers (#33898)``

4.1.4
.....

Misc
~~~~

* ``Refactor: Simplify code in Apache/Alibaba providers (#33227)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D401 Support - Providers: Airbyte to Atlassian (Inclusive) (#33354)``

4.1.3
.....

Bug Fixes
~~~~~~~~~

* ``Validate conn_prefix in extra field for Spark JDBC hook (#32946)``

4.1.2
.....

.. note::

    The provider now expects ``apache-airflow-providers-cncf-kubernetes`` in version 7.4.0+ installed
    in order to run Spark on Kubernetes jobs. You can install the provider with ``cncf.kubernetes`` extra with
    ``pip install apache-airflow-providers-spark[cncf.kubernetes]`` to get the right version of the
    ``cncf.kubernetes`` provider installed.

Misc
~~~~

* ``Move all k8S classes to cncf.kubernetes provider (#32767)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``D205 Support - Providers: Apache to Common (inclusive) (#32226)``
   * ``Improve provider documentation and README structure (#32125)``

4.1.1
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``SparkSubmitOperator: rename spark_conn_id to conn_id (#31952)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add D400 pydocstyle check - Apache providers only (#31424)``
   * ``Apache provider docstring improvements (#31730)``
   * ``Improve docstrings in providers (#31681)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``

4.1.0
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

4.0.1
.....

Bug Fixes
~~~~~~~~~

* ``Only restrict spark binary passed via extra (#30213)``
* ``Validate host and schema for Spark JDBC Hook (#30223)``
* ``Add spark3-submit to list of allowed spark-binary values (#30068)``

4.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

The ``spark-binary`` connection extra could be set to any binary, but with 4.0.0 version only two values
are allowed for it ``spark-submit`` and ``spark2-submit``.

The ``spark-home`` connection extra is not allowed anymore - the binary should be available on the
PATH in order to use SparkSubmitHook and SparkSubmitOperator.

* ``Remove custom spark home and custom binaries for spark (#27646)``

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add documentation for July 2022 Provider's release (#25030)``
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Update docs for September Provider's release (#26731)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
   * ``Prepare docs for new providers release (August 2022) (#25618)``
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Add typing for airflow/configuration.py (#23716)``
* ``Fix backwards-compatibility introduced by fixing mypy problems (#24230)``

Misc
~~~~

* ``AIP-47 - Migrate spark DAGs to new design #22439 (#24210)``
* ``chore: Refactoring and Cleaning Apache Providers (#24219)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

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

Bug Fixes
~~~~~~~~~

* ``fix param rendering in docs of SparkSubmitHook (#21788)``

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.1.0
.....

Features
~~~~~~~~

* ``Add more SQL template fields renderers (#21237)``
* ``Add optional features in providers. (#21074)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Add documentation for January 2021 providers release (#21257)``

2.0.3
.....

Bug Fixes
~~~~~~~~~

* ``Ensure Spark driver response is valid before setting UNKNOWN status (#19978)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
    * ``Fix mypy providers (#20190)``
    * ``Fix mypy spark hooks (#20290)``
    * ``Fix MyPy errors in Apache Providers (#20422)``
    * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
    * ``Even more typing in operators (template_fields/ext) (#20608)``
    * ``Update documentation for provider December 2021 release (#20523)``

2.0.2
.....

Bug Fixes
~~~~~~~~~

* ``fix bug of SparkSql Operator log  going to infinite loop. (#19449)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Cleanup of start_date and default arg use for Apache example DAGs (#18657)``
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``More f-strings (#18855)``
   * ``Remove unnecessary string concatenations in AirflowException messages (#18817)``

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
   * ``Updating Apache example DAGs to use XComArgs (#16869)``
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

Bug fixes
~~~~~~~~~

* ``Make SparkSqlHook use Connection (#15794)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.3
.....

Bug fixes
~~~~~~~~~

* ``Fix 'logging.exception' redundancy (#14823)``


1.0.2
.....

Bug fixes
~~~~~~~~~

* ``Use apache.spark provider without kubernetes (#14187)``


1.0.1
.....

Updated documentation and readme files.

1.0.0
.....

Initial version of the provider.
