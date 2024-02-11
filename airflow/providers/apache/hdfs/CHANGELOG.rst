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

``apache-airflow-providers-apache-hdfs``


Changelog
---------

4.3.2
.....

Misc
~~~~

* ``Remove _read method from hdfs task handler after bumping min airflow version to 2.6 (#36425)``
* ``Consolidate loading delete_local_logs conf in hdfs task handler (#36422)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.3.1
.....

Bug Fixes
~~~~~~~~~

* ``fix connection type webhdfs (#36145)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.3.0
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
   * ``Prepare docs 1st wave of Providers November 2023 (#35537)``
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Add information about Qubole removal and make it possible to release it (#35492)``

4.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Use 'airflow.exceptions.AirflowException' in providers (#34511)``

4.1.1
.....

Misc
~~~~

* ``Fix package name in exception message for hdfs provider (#33813)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use correct headings in HDFS Provider changelog (#33809)``
   * ``Prepare docs for Aug 2023 3rd wave of Providers (#33730)``
   * ``D401 Support - Providers: Airbyte to Atlassian (Inclusive) (#33354)``
   * ``Prepare docs for Aug 2023 2nd wave of Providers (#33291)``
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``D205 Support - Providers: Apache to Common (inclusive) (#32226)``
   * ``Improve provider documentation and README structure (#32125)``
   * ``Fix typos (double words and it's/its) (#33623)``

4.1.0
.....

.. note::
  This release dropped support for Python 3.7

Features
~~~~~~~~

* Add ability to read/write task instance logs from HDFS (#31512)

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Updates release notes for snakebite-py3 incompatibility with protobuf (#31756)``
   * ``Add D400 pydocstyle check - Apache providers only (#31424)``
   * ``Add note about dropping Python 3.7 for providers (#32015)``

4.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.


Breaking changes
~~~~~~~~~~~~~~~~

The original HDFS Hook and sensor has been removed. It used the old HDFS snakebite-py3 library that had no
update in years and the protobuf they are using reached end of life.

The 3.* version of the provider is still available and can be used if you need to use the old hooks and
sensors.

The ``HDFSHook``, ``HDFSSensor``, ``HdfsRegexSensor``, ``HdfsRegexSensor`` that have been removed from
this provider and they are not available anymore. If you want to continue using them,
you can use 3.* version of the provider, but the recommendation is to switch to the new
``WebHDFSHook`` and ``WebHDFSSensor`` that use the ``WebHDFS`` API.


* ``Remove snakebite-py3 based HDFS hooks and sensors (#31262)``


.. note::

   Protobuf 3 required by the snakebite-py3 library has ended its life in June 2023 and Airflow and it's
   providers stopped supporting it. If you would like to continue using HDFS hooks and sensors
   based on snakebite-py3 library when you have protobuf library 4.+ you can install the 3.* version
   of the provider but due to Protobuf incompatibility, you need to do one of the two things:

   * set ``PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python`` variable in your environment.
   * downgrade protobuf to latest 3.* version (3.20.3 at this time)

   Setting ``PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python`` will make many libraries using protobuf
   much slower - including multiple Google client libraries and Kubernetes. Downgrading protobuf to
   (already End-Of-Life) 3.* version will make some of the latest versions of the new providers
   incompatible (for example google and grpc) and you will have to downgrade those providers as well.
   Both should be treated as a temporary workaround only, and you should migrate to WebHDFS
   as soon as possible.


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

3.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix HDFSHook HAClient is invalid (#30164)``

3.2.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Update docs for September Provider's release (#26731)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

3.1.0
.....

Features
~~~~~~~~

* ``Adding Authentication to webhdfs sensor  (#25110)``

3.0.1
.....

Bug Fixes
~~~~~~~~~

* ``'WebHDFSHook' Bugfix/optional port (#24550)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* This release of provider is only available for Airflow 2.2+ as explained in the Apache Airflow
  providers support policy https://github.com/apache/airflow/blob/main/README.md#support-for-providers

Misc
~~~~

* ``chore: Refactoring and Cleaning Apache Providers (#24219)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.2.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.2.2
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.2.1
.....

Misc
~~~~

* ``Support for Python 3.10``
* ``Add how-to guide for WebHDFS operators (#21393)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Fix MyPy errors in Apache Providers (#20422)``
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.2.0
.....

Features
~~~~~~~~

* ``hdfs provider: restore HA support for webhdfs (#19711)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.1.1
.....

Bug Fixes
~~~~~~~~~

* ``fix get_connections deprecation warning in webhdfs hook (#18331)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

2.1.0
.....

Features
~~~~~~~~

* ``hdfs provider: allow SSL webhdfs connections (#17637)``

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Prepares docs for Rc2 release of July providers (#17116)``
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

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``Remove python2 related handlings and dependencies (#15301)``
   * ``Remove Backport Providers (#14886)``
   * ``Update documentation for broken package releases (#14734)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Update Docstrings of Modules with Missing Params (#15391)``
   * ``Created initial guide for HDFS operators  (#11212)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.0.1
.....

Updated documentation and readme files.


1.0.0
.....

Initial version of the provider.
