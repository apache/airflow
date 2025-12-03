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

``apache-airflow-providers-smtp``


Changelog
---------


2.4.0
.....

.. note::
    This release of provider is only available for Airflow 2.11+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Bug Fixes
~~~~~~~~~

* ``Include all __init__ params in template_fields for consistency with other operators (#58278)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.11.0 (#58612)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates to release process of providers (#58316)``

2.3.2
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``
* ``Migrate smtp provider to 'common.compat' (#57105)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable PT006 rule to 9 files in providers (snowflake,smtp/tests) (#57845)``
   * ``Prepare release for Oct 2025 wave of providers (#57029)``
   * ``Correct 'Dag' to 'DAG' for code snippets in provider docs (#56727)``
   * ``Remove placeholder Release Date in changelog and index files (#56056)``

2.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix import for SMTP provider (#56053)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.3.0
.....


Features
~~~~~~~~

* ``Add Async support for SMTP Notifier (#55308)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.2.1
.....


Bug Fixes
~~~~~~~~~

* ``Fix SMTP email template when mark_success_url is undefined for RuntimeTaskInstance objects (#54680)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Make smtp provider tests db independent (#54674)``
   * ``Replace API server's direct Connection access workaround in BaseHook (#54083)``
   * ``Switch pre-commit to prek (#54258)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

2.2.0
.....

Features
~~~~~~~~

* ``Add OAuth 2 / XOAUTH2 support via 'auth_type' & token/credential extras (#53554)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.1.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix: enter SmtpHook before trying to read properties (#53418)``

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup type ignores in smtp provider where possible (#53260)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Moving BaseHook usages to version_compat for smtp (#52950)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.1.1
.....

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Provider Migration: Replace 'models.BaseOperator' to Task SDK for 'smtp' (#52596)``
* ``Drop support for Python 3.9 (#52072)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Clean DB Test flag from SMTP where Connection is used (#52029)``
   * ``Introducing fixture to create 'Connections' without DB in provider tests (#51930)``

2.1.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Remove AIRFLOW_2_10_PLUS conditions (#49877)``
* ``Bump min Airflow version in providers to 2.10 (#49843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``
   * ``Replace chicken-egg providers with automated use of unreleased packages (#49799)``

2.0.3
.....

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
   * ``Upgrade ruff to latest version (#48553)``

2.0.2
.....

Misc
~~~~

* ``Move BaseNotifier to Task SDK (#48008)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``

2.0.1
.....

Misc
~~~~

* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  The argument ``from_email`` is now an optional kwarg in ``SmtpNotifier``, and the argument ``to`` became the first
  positional argument.

  Configuring the ``SmtpNotifier`` and ``SmtpHook`` default values via Airflow SMTP configurations is not supported
  anymore. You can instead use the SMTP connection configuration to set the default values, where you can use:

  * the connection extra field ``ssl_context`` instead of the configuration ``smtp_provider.ssl_context`` or
    ``email.ssl_context`` in the SMTP hook.
  * the connection extra field ``from_email`` instead of the configuration ``smtp.smtp_mail_from`` in ``SmtpNotifier``.
  * the connection extra field ``subject_template`` instead of the configuration ``smtp.templated_email_subject_path``
    in ``SmtpNotifier``.
  * the connection extra field ``html_content_template`` instead of the configuration
    ``smtp.templated_html_content_path`` in ``SmtpNotifier``.

* ``Replace Airflow email config by connection extras in SMTP provider (#46219)``

Features
~~~~~~~~

* ``feat(smtp): support html_content and subject templates from SMTP connection (#46212)``

Misc
~~~~

* ``AIP-72: Support better type-hinting for Context dict in SDK  (#45583)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Move smtp provider to the new structure (#46556)``

1.9.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``

1.8.1
.....

Misc
~~~~

* ``Move bash operator to Standard provider (#42252)``
* ``Purge existing SLA implementation (#42285)``
* ``Unify DAG schedule args and change default to None (#41453)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

1.8.0
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

1.7.1
.....

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

1.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave (RC1) April 2024 (#38863)``
   * ``Update yanked versions in providers changelogs (#38262)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``bump mypy version to 1.9.0 (#38239)``

1.6.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix backwards compatibility for SMTP provider (#37701)``

Misc
~~~~

* ``Deprecate smtp configs in airflow settings / local_settings (#37711)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add comment about versions updated by release manager (#37488)``
   * ``Added D401 support to http, smtp and sftp provider (#37303)``
   * ``Add docs for RC2 wave of providers for 2nd round of Jan 2024 (#37019)``
   * ``Revert "Provide the logger_name param in providers hooks in order to override the logger name (#36675)" (#37015)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``
   * ``Provide the logger_name param in providers hooks in order to override the logger name (#36675)``
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``

1.6.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``The release had broken backwards compatibility with Airflow 2.8.* release``

Features
~~~~~~~~

* ``Modify SmtpNotifier to accept template with defaults (#36226)``

Bug Fixes
~~~~~~~~~

* ``Follow BaseHook connection fields method signature in child classes (#36086)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.5.0
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
   * ``Use reproducible builds for providers (#35693)``

1.4.1
.....

Misc
~~~~

* ``Make 'cc' and 'bcc' templated fields in EmailOperator (#35235)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Upgrade pre-commits (#35033)``

1.4.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``

1.3.2
.....

Misc
~~~~

* ``Refactor unneeded  jumps in providers (#33833)``
* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``

1.3.1
.....

Bug Fixes
~~~~~~~~~

* ``Simplify 'X for X in Y' to 'Y' where applicable (#33453)``

1.3.0
.....

In case of SMTP SSL connection, the default context now uses "default" context

The "default" context is Python's ``default_ssl_context`` instead of previously used "none". The
``default_ssl_context`` provides a balance between security and compatibility but in some cases,
when certificates are old, self-signed or misconfigured, it might not work. This can be configured
by setting "ssl_context" in "smtp_provider" configuration of the provider. If it is not explicitly set,
it will default to "email", "ssl_context" setting in Airflow.

Setting it to "none" brings back the "none" setting that was used in previous versions of the provider,
but it is not recommended due to security reasons ad this setting disables validation
of certificates and allows MITM attacks.

You can also override "ssl_context" per-connection by setting "ssl_context" in the connection extra.

Features
~~~~~~~~

* ``Add possibility to use 'ssl_context' extra for SMTP and IMAP connections (#33112)``
* ``Allows to choose SSL context for SMTP provider (#33075)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
   * ``D205 Support - Providers: Pagerduty to SMTP (inclusive) (#32358)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Improve provider documentation and README structure (#32125)``

1.2.0
.....

.. note::
  This release dropped support for Python 3.7

Features
~~~~~~~~

* ``Add notifier for Smtp (#31359)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add D400 pydocstyle check - Providers (#31427)``
   * ``Fix ruff static check (#31762)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``

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
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

1.0.1
.....

Bug Fixes
~~~~~~~~~

* ``'EmailOperator': fix wrong assignment of 'from_email' (#30524)``
* ``Accept None for 'EmailOperator.from_email' to load it from smtp connection (#30533)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add mechanism to suspend providers (#30422)``

1.0.0
.....

Initial version of the provider.
