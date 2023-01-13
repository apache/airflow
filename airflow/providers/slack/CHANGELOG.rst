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

This release of provider is only available for Airflow 2.3+ as explained in the
`Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/README.md#support-for-providers>`_.

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

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

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
