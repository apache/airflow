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
   * ``Use reproducible builds for provider packages (#35693)``

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
