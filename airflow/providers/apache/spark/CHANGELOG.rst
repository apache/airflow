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
