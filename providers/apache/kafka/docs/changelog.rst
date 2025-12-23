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


``apache-airflow-providers-apache-kafka``


Changelog
---------

1.11.1
......

Misc
~~~~

* ``Add backcompat for exceptions in providers (#58727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.11.0
......

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Bug Fixes
~~~~~~~~~

* ``Fix AwaitMessageSensor to accept timeout and soft_fail parameters (#57863) (#58070)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``

1.10.6
......

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable PT006 rule to 13 files in providers (apache) (#57998)``
   * ``Fix documentation/provider.yaml consistencies (#57283)``

1.10.5
......

Misc
~~~~

* ``Make 'AwaitMessageTrigger' inherit 'BaseEventTrigger' (#56741)``
* ``Migrate Apache providers & Elasticsearch to ''common.compat'' (#57016)``

Doc-only
~~~~~~~~


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix pytest collection failure for classes decorated with context managers (#55915)``
   * ``Remove placeholder Release Date in changelog and index files (#56056)``

1.10.4
......


Bug Fixes
~~~~~~~~~

* ``Make 'apply_function' optional in 'AwaitMessageTrigger' (#55437)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.10.3
......


Misc
~~~~

* ``Refactor Common Queue Interface (#54651)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Switch pre-commit to prek (#54258)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

1.10.2
......

Misc
~~~~

* ``Set minimum version for common.messaging to 1.0.3 (#54160)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix 'importskip' statements in tests (#54135)``

1.10.1
......

Bug Fixes
~~~~~~~~~

* ``Add validation for commit_cadence in Kafka ConsumeFromTopicOperator (#52015)``

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup type ignores (#53300)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Make kafka provider compatible with mypy 1.16.1 (#53125)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Replace BaseHook to Task SDK for apache/kafka (#52784)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.10.0
......

Features
~~~~~~~~

* ``Add 'KafkaMessageQueueTrigger' for enhanced message queue trigger usability on Kafka queue (#51718)``

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Update BaseOperator imports for Airflow 3.0 compatibility (#52503)``
* ``Drop support for Python 3.9 (#52072)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove pytest db marker apache kafka (#52098)``
   * ``Updating kafka systests to setup connections using ENV (#52076)``
   * ``Replace usage of os.environ with conf_vars in kafka IT (#52025)``
   * ``Introducing fixture to create 'Connections' without DB in provider tests (#51930)``

1.9.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Features
~~~~~~~~

* ``AIP-82: Add KafkaMessageQueueProvider (#49938)``

Bug Fixes
~~~~~~~~~

* ``fix: correct invalid arg timeout (#49426)``
* ``Fix max_messages warning of Kafka ConsumeFromTopicOperator (#48646)``

Misc
~~~~

* ``Bump min Airflow version in providers to 2.10 (#49843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``

1.8.1
.....

Misc
~~~~

* ``remove superfluous else block (#49199)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Apr 2nd wave of providers (#49051)``
   * ``Fix false friends in implicit string concatenation (#48871)``
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``
   * ``Prepare docs for Apr 1st wave of providers (#48828)``
   * ``Simplify tooling by switching completely to uv (#48223)``

1.8.0
.....

Features
~~~~~~~~

* ``Create operators for working with Consumer Groups for GCP Apache Kafka (#47056)``

Misc
~~~~

* ``Remove google provider dependencies from apache-kafka provider (#47563)``
* ``Remove exclusions of confluent-kafka (#47240)``
* ``Limit confluent-kafka temporarily to exclude 2.8.1 (#47204)``
* ``Add legacy namespace packages to airflow.providers (#47064)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Deprecating email, email_on_retry, email_on_failure in BaseOperator (#47146)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Fix kafka connection refuse issue in kafka provider tests (#47213)``
   * ``Remove extra whitespace in provider readme template (#46975)``
   * ``Upgrade flit to 3.11.0 (#46938)``
   * ``Prepare docs for Feb 1st wave of providers (#46893)``
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Fix doc issues found with recent moves (#46372)``
   * ``Fix Kafka provider tests after move (#46214)``
   * ``Move Apache Kafka to new provider structure (#46110)``

1.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``fix 'ConsumeFromTopicOperator' does not fail even if wrong credentials (#44307)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Update DAG example links in multiple providers documents (#44034)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Prepare docs for Nov 1st wave of providers (#44011)``
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``
   * ``Update path of example dags in docs (#45069)``

1.6.1
.....

Bug Fixes
~~~~~~~~~

* ``remove callable functions parameter from kafka operator template_fields (#42555)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.6.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Aug 1st wave of providers (#41230)``
   * ``Prepare docs 1st wave July 2024 (#40644)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

1.5.0
.....

Features
~~~~~~~~

* ``Add 'delete_topic' to 'KafkaAdminClientHook' (#40142)``

Bug Fixes
~~~~~~~~~

* ``Default client in KafkaBaseHook (#40284)``

Misc
~~~~

* ``implement per-provider tests with lowest-direct dependency resolution (#39946)``

1.4.1
.....

Bug Fixes
~~~~~~~~~

* ``Group id is mandatory configuration option for confluent_kafka 2.4.0+ (#39559)``

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

1.4.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add comment about versions updated by release manager (#37488)``
   * ``Add docs for RC2 wave of providers for 2nd round of Jan 2024 (#37019)``
   * ``Prepare docs 1st wave (RC1) April 2024 (#38863)``
   * ``Resolve G004: Logging statement uses f-string (#37873)``
   * ``Prepare docs 1st wave (RC1) March 2024 (#37876)``
   * ``Prepare docs 1st wave of Providers February 2024 (#37326)``
   * ``Revert "Provide the logger_name param in providers hooks in order to override the logger name (#36675)" (#37015)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``
   * ``Provide the logger_name param in providers hooks in order to override the logger name (#36675)``
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``

1.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Provide the consumed message to consumer.commit in AwaitMessageTrigger (#36272)``
* ``Follow BaseHook connection fields method signature in child classes (#36086)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.3.0
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
   * ``Prepare docs 1st wave of Providers November 2023 (#35537)``
   * ``Switch from Black to Ruff formatter (#35287)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Upgrade pre-commits (#35033)``

1.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Fix typos (double words and it's/its) (#33623)``
* ``Fixes the Kafka provider's max message limit error (#32926) (#33321)``

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Use 'airflow.exceptions.AirflowException' in providers (#34511)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Aug 2023 3rd wave of Providers (#33730)``
   * ``D401 Support - Providers: Airbyte to Atlassian (Inclusive) (#33354)``

1.1.2
.....

Bug Fixes
~~~~~~~~~

* ``Break AwaitMessageTrigger execution when finding a message with the desired format (#31803)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D205 Support - Providers: Apache to Common (inclusive) (#32226)``
   * ``Improve provider documentation and README structure (#32125)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``

1.1.1
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``Remove Python 3.7 support (#30963)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add discoverability for triggers in provider.yaml (#31576)``
   * ``Add D400 pydocstyle check - Apache providers only (#31424)``
   * ``Add note about dropping Python 3.7 for kafka and impala (#32017)``

1.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers (#30917)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade ruff to 0.0.262 (#30809)``
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Add cli cmd to list the provider trigger info (#30822)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

1.0.0
.....

Initial version of the provider.
