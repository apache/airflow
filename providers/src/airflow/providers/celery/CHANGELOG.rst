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
   * ``Use reproducible builds for provider packages (#35693)``
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

.. Review and move the new changes to one of the sections above:
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
