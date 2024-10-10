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

``apache-airflow-providers-slack``


Changelog
---------

8.9.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

8.8.0
.....

Features
~~~~~~~~

* ``feat(slack): add unfurl options to slack notifier (#40694)``

Misc
~~~~

* ``docs(SlackNotifier): add newly added unfurl args to the docstring (#40709)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave July 2024 (#40644)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

8.7.1
.....

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

8.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

.. note::
  Due to future discontinue of `files.upload <https://api.slack.com/changelog/2024-04-a-better-way-to-upload-files-is-here-to-stay>`__
  Slack API method the default value of ``SlackAPIFileOperator.method_version`` and ``SqlToSlackApiFileOperator.slack_method_version``
  changed from ``v1`` to ``v2``

  If you previously use ``v1`` you should check that your application has appropriate scopes:

  * **files:write** - for write files.
  * **files:read** - for read files (not required if you use Slack SDK >= 3.23.0).
  * **channels:read** - get list of public channels, for convert Channel Name to Channel ID.
  * **groups:read** - get list of private channels, for convert Channel Name to Channel ID
  * **mpim:read** - additional permission for API method **conversations.list**
  * **im:read** - additional permission for API method **conversations.list**

  If you use ``SlackHook.send_file`` please consider switch to ``SlackHook.send_file_v2``
  or ``SlackHook.send_file_v1_to_v2`` methods.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``
* ``Use 'upload_files_v2' Slack SDK method by default in Slack Operators (#39340)``

8.6.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix set deprecated slack operators arguments in 'MappedOperator' (#38345)``
* ``Update SqlToSlackApiFileOperator with new param to check empty output (#38079)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

   * ``Bump ruff to 0.3.3 (#38240)``
   * ``Prepare docs 1st wave (RC1) March 2024 (#37876)``
   * ``Avoid to use too broad 'noqa' (#37862)``
   * ``Add comment about versions updated by release manager (#37488)``

8.6.1
.....

Misc
~~~~

* ``Remove the remaining references to use the token bypassing the Connection in the Slack provider. (#37112)``
* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Follow D401 style in openlineage, slack, and tableau providers (#37312)``

8.6.0
.....

Features
~~~~~~~~

* ``Optionally use 'client.files_upload_v2' in Slack Provider (#36757)``

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

8.5.1
.....

Bug Fixes
~~~~~~~~~

* ``Slack: Remove parameter 'token' in SlackAPIPostOperator's docstring (#36121)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

8.5.0
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
   * ``Use reproducible builds for provider packages (#35693)``

8.4.0
.....

Features
~~~~~~~~

* ``Reorganize SQL to Slack Operators (#35215)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Work around typing issue in examples and providers (#35494)``
   * ``Add missing examples into Slack Provider (#35495)``

8.3.0
.....

Features
~~~~~~~~

* ``Pass additional arguments from Slack's Operators/Notifiers to Hooks (#35039)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Upgrade pre-commits (#35033)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``

8.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Slack: use default_conn_name by default (#34548)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Docstring correction for 'SlackAPIOperator' (#34871)``

8.1.0
.....

Features
~~~~~~~~

* ``Add Slack Incoming Webhook Notifier (#33966)``

Misc
~~~~

* ``Refactor: Replace lambdas with comprehensions in providers (#33771)``
* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``

8.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  ``SlackHook`` and ``SlackWebhookHook`` constructor expected keyword-only arguments.

  Removed deprecated parameter ``token`` from the ``SlackHook`` and dependent operators.
  Required create ``Slack API Connection`` and provide connection id to ``slack_conn_id`` operators / hook,
  and the behavior should stay the same.

  Parsing Slack Incoming Webhook Token from the Connection ``hostname`` is removed, ``password`` should be filled.

  Removed deprecated parameter ``webhook_token`` from the ``SlackWebhookHook`` and dependent operators
  Required create ``Slack Incoming Webhook Connection`` and provide connection id to ``slack_webhook_conn_id``
  operators / hook, and the behavior should stay the same.

  Removed deprecated method ``execute`` from the ``SlackWebhookHook``. Use ``send``, ``send_text`` or ``send_dict`` instead.

  Removed deprecated parameters ``attachments``, ``blocks``, ``channel``, ``username``, ``username``,
  ``icon_emoji`` from the ``SlackWebhookHook``. Provide them directly to ``SlackWebhookHook.send`` method,
  and the behavior should stay the same.

  Removed deprecated parameter ``message`` from the ``SlackWebhookHook``.
  Provide ``text`` directly to ``SlackWebhookHook.send`` method, and the behavior should stay the same.

  Removed deprecated parameter ``link_names`` from the ``SlackWebhookHook`` and dependent operators.
  This parameter has no affect in the past, you should not provide it.
  If you want to mention user see: `Slack Documentation <https://api.slack.com/reference/surfaces/formatting#mentioning-users>`__.

  Removed deprecated parameters ``endpoint``, ``method``, ``data``, ``headers``, ``response_check``,
  ``response_filter``, ``extra_options``, ``log_response``, ``auth_type``, ``tcp_keep_alive``,
  ``tcp_keep_alive_idle``, ``tcp_keep_alive_idle``, ``tcp_keep_alive_count``, ``tcp_keep_alive_interval``
  from the ``SlackWebhookOperator``. Those parameters has no affect in the past, you should not provide it.

* ``Remove deprecated parts from Slack provider (#33557)``
* ``Replace deprecated slack notification in provider.yaml with new one (#33643)``

Misc
~~~~

* ``Avoid importing pandas and numpy in runtime and module level (#33483)``
* ``Consolidate import and usage of pandas (#33480)``

7.3.2
.....

Misc
~~~~

* ``Add more accurate typing for DbApiHook.run method (#31846)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
   * ``D205 Support - Providers: Pagerduty to SMTP (inclusive) (#32358)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``Improve provider documentation and README structure (#32125)``

7.3.1
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

7.3.0
.....

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers (#30917)``
* ``Add Documentation for notification feature extension (#29191)``
* ``Standardize Slack Notifier (#31244)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use 'AirflowProviderDeprecationWarning' in providers (#30975)``
   * ``Prepare docs for Feb 2023 wave of Providers (#29379)``
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Add mechanism to suspend providers (#30422)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

7.2.0
.....

Features
~~~~~~~~

* ``Add general-purpose "notifier" concept to DAGs (#28569)``

7.1.1
.....

Misc
~~~~

* ``[misc] Replace XOR '^' conditions by 'exactly_one' helper in providers (#27858)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

7.1.0
.....

Features
~~~~~~~~

* ``Implements SqlToSlackApiFileOperator (#26374)``

Bug Fixes
~~~~~~~~~

* ``Bump common.sql provider to 1.3.1 (#27888)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare for follow-up release for November providers (#27774)``

7.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

* In SlackHook and SlackWebhookHook, if both ``extra__<conn type>__foo`` and ``foo`` existed
  in connection extra dict, the prefixed version would be used; now, the non-prefixed version
  will be preferred.  You'll see a warning if there is such a collision.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``
* ``Allow and prefer non-prefixed extra fields for slack hooks (#27070)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable string normalization in python formatting - providers (#27205)``

.. Review and move the new changes to one of the sections above:
   * ``Replace urlparse with urlsplit (#27389)``

6.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* The hook class  ``SlackWebhookHook`` does not inherit from ``HttpHook`` anymore. In practice the
  only impact on user-defined classes based on **SlackWebhookHook** and you use attributes
  from **HttpHook**.
* Drop support deprecated ``webhook_token`` parameter in ``slack-incoming-webhook`` extra.

* ``Refactor 'SlackWebhookOperator': Get rid of mandatory http-provider dependency (#26648)``
* ``Refactor SlackWebhookHook in order to use 'slack_sdk' instead of HttpHook methods (#26452)``

Features
~~~~~~~~

* ``Move send_file method into SlackHook (#26118)``
* ``Refactor Slack API Hook and add Connection (#25852)``
* ``Remove unsafe imports in Slack API Connection (#26459)``
* ``Add common-sql lower bound for common-sql (#25789)``
* ``Fix Slack Connections created in the UI (#26845)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
   * ``Replace SQL with Common SQL in pre commit (#26058)``
   * ``Prepare docs for new providers release (August 2022) (#25618)``
   * ``AIP-47 - Migrate Slack DAG to new design (#25137)``
   * ``Fix errors in CHANGELOGS for slack and amazon (#26746)``
   * ``Update docs for September Provider's release (#26731)``

5.1.0
.....

Features
~~~~~~~~

* ``Move all SQL classes to common-sql provider (#24836)``
* ``Adding generic 'SqlToSlackOperator' (#24663)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update docstring in 'SqlToSlackOperator' (#24759)``
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

5.0.0
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

4.2.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

4.2.2
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

4.2.1
.....

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.2.0
.....

Features
~~~~~~~~

* ``Return slack api call response in slack_hook (#21107)``

Bug Fixes
~~~~~~~~~

* ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Fix mypy providers (#20190)``
   * ``Doc: Restoring additional context in Slack operators how-to guide (#18985)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Update documentation for provider December 2021 release (#20523)``
   * ``Update SlackWebhookHook docstring (#20061)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Update documentation for November 2021 provider's release (#19882)``
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``Add documentation for January 2021 providers release (#21257)``

4.1.0
.....


Features
~~~~~~~~

* ``Restore filename to template_fields (#18466)``

Bug Fixes
~~~~~~~~~


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
   * ``Add Slack operators how-to guide (#18525)``


4.0.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

Bug Fixes
~~~~~~~~~

* ``Fixed SlackAPIFileOperator to upload file and file content. (#17400)``
* ``Fixed SlackAPIFileOperator to upload file and file content (#17247)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Prepares docs for Rc2 release of July providers (#17116)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Removes pylint from our toolchain (#16682)``

4.0.0
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
   * ``Adds interactivity when generating provider documentation. (#15518)``
   * ``Rename the main branch of the Airflow repo to be 'main' (#16149)``
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``Remove Backport Providers (#14886)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Fix Sphinx Issues with Docstrings (#14968)``
   * ``Fix docstring formatting on ``SlackHook`` (#15840)``
   * ``Add Connection Documentation for Providers (#15499)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Don't allow SlackHook.call method accept *args (#14289)``


2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

We updated the support for ``slack_sdk`` from ``>=2.0.0,<3.0.0`` to ``>=3.0.0,<4.0.0``. In most cases,
this doesn't mean any breaking changes to the DAG files, but if you used this library directly
then you have to make the changes. For details, see
`the Migration Guide <https://slack.dev/python-slack-sdk/v3-migration/index.html#from-slackclient-2-x>`_
for Python Slack SDK.

* ``Upgrade slack_sdk to v3 (#13745)``


1.0.0
.....

Initial version of the provider.
