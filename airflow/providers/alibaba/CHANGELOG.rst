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

``apache-airflow-providers-alibaba``

Changelog
---------

2.9.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.8.1
.....

Bug Fixes
~~~~~~~~~

* ``fix oss key prefix bug in oss_task_handler (#39627)``

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

2.8.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

2.7.3
.....

Bug Fixes
~~~~~~~~~

* ``Avoid use of 'assert' outside of the tests (#37718)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``fix: try002 for provider alibaba (#38787)``
   * ``Prepare docs 1st wave (RC1) March 2024 (#37876)``
   * ``Add comment about versions updated by release manager (#37488)``

2.7.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix assignment of template field in '__init__' in 'analyticdb_spark.py' (#36911)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

2.7.1
.....

Misc
~~~~

* ``Clean OSS task handler code after bumping min Airflow version to 2.6.0 (#36420)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Re-apply updated version numbers to 2nd wave of providers in December (#36380)``

2.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update information about links into the provider.yaml files (#35837)``
   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for provider packages (#35693)``
   * ``Prepare docs 1st wave of Providers November 2023 (#35537)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Fix and reapply templates for provider documentation (#35686)``
   * ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``


2.6.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Consolidate hook management in AnalyticDBSparkSensor (#34435)``
* ``Consolidate hook management in AnalyticDBSparkBaseOperator (#34434)``
* ``Deprecate get_hook in OSSKeySensor and use hook instead (#34426)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor: consolidate import time in providers (#34402)``
   * ``Refactor usage of str() in providers (#34320)``

2.5.3
.....

Bug Fixes
~~~~~~~~~

* ``fix(providers/alibaba): respect soft_fail argument when exception is raised (#34157)``

2.5.2
.....

Misc
~~~~

* ``Refactor: Simplify code in Apache/Alibaba providers (#33227)``
* ``Simplify conditions on len() in providers/apache (#33564)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D401 Support - Providers: Airbyte to Atlassian (Inclusive) (#33354)``

2.5.1
.....

Misc
~~~~

* ``Refactor: Simplify code in providers/alibaba (#33225)``

2.5.0
.....

Features
~~~~~~~~

* ``Add Alibaba Cloud AnalyticDB Spark Support (#31787)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D205 Support - Providers: Airbyte and Alibaba (#32214)``
   * ``Improve provider documentation and README structure (#32125)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``

2.4.1
.....

.. note::
  This release dropped support for Python 3.7


Misc
~~~~

* ``Remove Python 3.7 support (#30963)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add D400 pydocstyle check - Providers (#31427)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``

2.4.0
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

2.3.0
.....

Features
~~~~~~~~

* ``Support deleting the local log files when using remote logging (#29772)``

2.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``
* ``Use log.exception where more economical than log.error (#27517)``
* ``Replace urlparse with urlsplit (#27389)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``

2.1.0
.....

Features
~~~~~~~~

* ``Auto tail file logs in Web UI (#26169)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

2.0.1
.....

Bug Fixes
~~~~~~~~~

* ``Update providers to use functools compat for ''cached_property'' (#24582)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

   * ``SSL Bucket, Light Logic Refactor and Docstring Update for Alibaba Provider (#23891)``

Misc
~~~~

   * ``Apply per-run log templates to log handlers (#24153)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate Alibaba example DAGs to new design #22437 (#24130)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

1.1.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

1.1.0
.....

Features
~~~~~~~~

* ``Add oss_task_handler into alibaba-provider and enable remote logging to OSS (#21785)``

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

1.0.1
.....

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Adding missing init file in example_dags directory (#18019)``
   * ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Fix MyPy Errors for Alibaba provider. (#20393)``
   * ``Fix cached_property MyPy declaration and related MyPy errors (#20226)``
   * ``Fixup string concatenations (#19099)``
   * ``Remove a few stray ':type's in docs (#21014)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Remove all "fake" stub files (#20936)``
   * ``Explain stub files are introduced for Mypy errors in examples (#20827)``
   * ``Update documentation for provider December 2021 release (#20523)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Use isort on pyi files (#20556)``
   * ``Reinstate 'region' to 'default_args' for Alibaba example DAGs (#20423)``
   * ``Update documentation for November 2021 provider's release (#19882)``
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``Update documentation for September providers release (#18613)``
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``

1.0.0
.....

Initial version of the provider.
