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

``apache-airflow-providers-apache-beam``

Changelog
---------

6.2.1
.....

Misc
~~~~

* ``Add backcompat for exceptions in providers (#58727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.2.0
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
   * ``Move Airflow Config Parser to shared library (#57744)``

6.1.7
.....

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

Doc-only
~~~~~~~~

* ``[Doc] Fixing some typos and spelling errors (#57225)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Enable ruff PLW2101,PLW2901,PLW3301 rule (#57700)``
   * ``Enable PT006 rule to 13 files in providers (apache) (#57998)``

6.1.6
.....

Bug Fixes
~~~~~~~~~

* ``Fix broken DataflowJobLink for Java operators (#56286)``

Misc
~~~~

* ``Migrate Apache providers & Elasticsearch to ''common.compat'' (#57016)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove placeholder Release Date in changelog and index files (#56056)``

6.1.5
.....


Bug Fixes
~~~~~~~~~

* ``disable links for beam provider operators using runner (#55248)``
* ``Fix dataflow java streaming infinite run (#55209)``
* ``remove the 'provide_authorized_glcoud' from BeamPipeline operators (#55453)``

Misc
~~~~

* ``Switch all airflow logging to structlog (#52651)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.1.4
.....


Bug Fixes
~~~~~~~~~

* ``fix apache beam provider with missing google provider (#54982)``

Doc-only
~~~~~~~~

* ``Make term Dag consistent in providers docs (#55101)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move trigger_rule utils from 'airflow/utils'  to 'airflow.task'and integrate with Execution API spec (#53389)``
   * ``Switch pre-commit to prek (#54258)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``
   * ``switch to DataflowJobStateCompleteTrigger to work with modern google-provider (#55156)``

6.1.3
.....

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Cleanup type ignores (#53303)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Replace BaseHook to Task SDK for apache/beam (#52781)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.1.2
.....

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Bump pyarrow to 16.1.0 minimum version for several providers (#52635)``
* ``Provider Migration: Update beam for Airflow 3.0 compatibility (#52607)``
* ``Drop support for Python 3.9 (#52072)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Make sure all test version imports come from test_common (#52425)``
   * ``Remove pytest.mark.db_test: apache beam (#52059)``

6.1.1
.....

Misc
~~~~

* ``Bump apache-beam>=2.60.0 (#51961)``
* ``Change the Link implementation in Google Provider to make it cleand and compatible with Airflow 3 (#51576)``
* ``Bump apache-beam minimum version dependency to 2.60.0 (#51825)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

6.1.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Features
~~~~~~~~

* ``Add new DataflowJobStateCompleteTrigger (#47993)``

Misc
~~~~

* ``Bump min Airflow version in providers to 2.10 (#49843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``

6.0.4
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
   * ``Prepare docs for Mar 2nd wave of providers (#48383)``
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``

6.0.3
.....

Bug Fixes
~~~~~~~~~

* ``Fix beam pipeline options False value parsing (#47419)``

Misc
~~~~

* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

6.0.2
.....

.. note::
  This version has no code changes. It's released due to yank of previous version due to packaging issues.

6.0.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix DataflowJobLink for Beam operators in deferrable mode (#45023)``

Misc
~~~~

* ``Set minimum dependencies for apache-beam on Py 3.12+3.13 (#46321)``
* ``Limit Apache Beam's numpy used (#46286)``
* ``Refactor deferrable mode for BeamRunPythonPipelineOperator and BeamRunJavaPipelineOperator (#46678)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Fix doc issues found with recent moves (#46372)``
   * ``Fix example import tests after move of providers to new structure (#46217)``
   * ``Fixing apache beam system tests import failure after new structure changes (#46201)``
   * ``Moved apache beam provider to new folder structure (#46071)``
   * ``update outdated hyperlinks referencing provider package files (#45332)``

6.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  All deprecated classes, parameters, and features have been removed from the Airbyte provider package.
  The following breaking changes were introduced:

  * Removed ``BeamPipelineTrigger`` class from ``trigger``. Use the ``class:`airflow.providers.apache.beam.triggers.beam.BeamPythonPipelineTrigger`` class instead.

* ``Removed deprecated code (#44700)``

Bug Fixes
~~~~~~~~~

* ``Fix deferrable mode of BeamRunPythonPipelineOperator (#44386)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Update DAG example links in multiple providers documents (#44034)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``
   * ``Update path of example dags in docs (#45069)``

5.9.1
.....

Misc
~~~~

* ``Standard provider python operator (#42081)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.9.0
.....

Features
~~~~~~~~

* ``Add early job_id xcom_push for google provider Beam Pipeline operators (#42982)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Consistent python version checks and troubleshooting (#42944)``
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

5.8.1
.....

Bug Fixes
~~~~~~~~~

* ``Bugfix/dataflow job location passing (#41887)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.8.0
.....

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.7.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix BeamRunJavaPipelineOperator fails without job_name set (#40645)``

5.7.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix deferrable mode for BeamRunJavaPipelineOperator (#39371)``

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

5.7.0
.....

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Bugfix to correct GCSHook being called even when not required with BeamRunPythonPipelineOperator (#38716)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

5.6.3
.....

Bug Fixes
~~~~~~~~~

* ``fix: skip apache beam pipeline options if value is set to false (#38496)``
* ``Fix side-effect of default options in Beam Operators (#37916)``
* ``Avoid to use subprocess in asyncio loop (#38292)``
* ``Avoid change attributes into the constructor in Apache Beam operators (#37934)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``fix: try002 for provider apache beam (#38790)``
   * ``Bump ruff to 0.3.3 (#38240)``
   * ``bump mypy version to 1.9.0 (#38239)``
   * ``Resolve G004: Logging statement uses f-string (#37873)``

5.6.2
.....

Misc
~~~~

* ``Add Python 3.12 exclusions in providers/pyproject.toml (#37404)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add comment about versions updated by release manager (#37488)``

5.6.1
.....

Misc
~~~~

* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split contributing docs to multiple files (#36969)``

5.6.0
.....

Misc
~~~~

* ``Get rid of pyarrow-hotfix for CVE-2023-47248 (#36697)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Standardize airflow build process and switch to Hatchling build backend (#36537)``
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Provide the logger_name param in providers hooks in order to override the logger name (#36675)``
   * ``Revert "Provide the logger_name param in providers hooks in order to override the logger name (#36675)" (#37015)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

5.5.0
.....

Features
~~~~~~~~

* ``Add ability to run streaming Job for BeamRunPythonPipelineOperator in non deferrable mode (#36108)``
* ``Implement deferrable mode for BeamRunJavaPipelineOperator (#36122)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

5.4.0
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
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``

5.3.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``
* ``Use 'airflow.exceptions.AirflowException' in providers (#34511)``


5.2.3
.....

Misc
~~~~

* ``Replace sequence concatenation by unpacking in Airflow providers (#33933)``
* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``

5.2.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix wrong OR condition when evaluating beam version < 2.39.0 (#33308)``

Misc
~~~~

* ``Refactor: Simplify code in Apache/Alibaba providers (#33227)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``D401 Support - Providers: Airbyte to Atlassian (Inclusive) (#33354)``
   * ``D205 Support - Providers - Final Pass (#33303)``

5.2.1
.....

Misc
~~~~

* ``Allow downloading requirements file from GCS in 'BeamRunPythonPipelineOperator' (#31645)``

5.2.0
.....

Features
~~~~~~~~

* ``Add deferrable mode to 'BeamRunPythonPipelineOperator' (#31471)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``D205 Support - Providers: Apache to Common (inclusive) (#32226)``
   * ``Improve provider documentation and README structure (#32125)``

5.1.1
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``Add note about dropping Python 3.7 for providers (#32015)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add D400 pydocstyle check - Apache providers only (#31424)``

5.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers (#30917)``
* ``Update SDKs for google provider package (#30067)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

5.0.0
......

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  In this version of the provider, deprecated GCS and Dataflow hooks' param ``delegate_to`` is removed from all Beam operators.
  Impersonation can be achieved instead by utilizing the ``impersonation_chain`` param.

* ``remove delegate_to from GCP operators and hooks (#30748)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add mechanism to suspend providers (#30422)``

4.3.0
.....

Features
~~~~~~~~

* ``Get rid of state in Apache Beam provider hook (#29503)``

4.2.0
.....

Features
~~~~~~~~

* ``Add support for running a Beam Go pipeline with an executable binary (#28764)``

Misc
~~~~
* ``Deprecate 'delegate_to' param in GCP operators and update docs (#29088)``

4.1.1
.....

Bug Fixes
~~~~~~~~~
* ``Ensure Beam Go file downloaded from GCS still exists when referenced (#28664)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

Features
~~~~~~~~

* ``Add backward compatibility with old versions of Apache Beam (#27263)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add documentation for July 2022 Provider's release (#25030)``
   * ``Update old style typing (#26872)``
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Update docs for September Provider's release (#26731)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``
   * ``Prepare docs for new providers release (August 2022) (#25618)``
   * ``Move provider dependencies to inside provider folders (#24672)``

4.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Added missing project_id to the wait_for_job (#24020)``
* ``Support impersonation service account parameter for Dataflow runner (#23961)``

Misc
~~~~

* ``chore: Refactoring and Cleaning Apache Providers (#24219)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``AIP-47 - Migrate beam DAGs to new design #22439 (#24211)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

3.4.0
.....

Features
~~~~~~~~

* ``Support serviceAccount attr for dataflow in the Apache beam``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.3.0
.....

Features
~~~~~~~~

* ``Add recipe for BeamRunGoPipelineOperator (#22296)``

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

3.2.1
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

3.2.0
.....

Features
~~~~~~~~

* ``Add support for BeamGoPipelineOperator (#20386)``

Misc
~~~~

* ``Support for Python 3.10``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
   * ``Fix mypy apache beam operators (#20610)``
   * ``Fix K8S changelog to be PyPI-compatible (#20614)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Fix MyPy Errors for Apache Beam (and Dataflow) provider. (#20301)``
   * ``Fix broken anchors markdown files (#19847)``
   * ``Add documentation for January 2021 providers release (#21257)``
   * ``Dataflow Assets (#21639)``
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Update documentation for provider December 2021 release (#20523)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Update documentation for November 2021 provider's release (#19882)``
   * ``Cleanup of start_date and default arg use for Apache example DAGs (#18657)``

3.1.0
.....

Features
~~~~~~~~

* ``Use google cloud credentials when executing beam command in subprocess (#18992)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

3.0.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
   * ``Prepares docs for Rc2 release of July providers (#17116)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Removes pylint from our toolchain (#16682)``

3.0.0
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
   * ``Rename the main branch of the Airflow repo to be main (#16149)``
   * ``Check synctatic correctness for code-snippets (#16005)``
   * ``Rename example bucket names to use INVALID BUCKET NAME by default (#15651)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

Integration with the ``google`` provider
````````````````````````````````````````

In 2.0.0 version of the provider we've changed the way of integrating with the ``google`` provider.
The previous versions of both providers caused conflicts when trying to install them together
using PIP > 20.2.4. The conflict is not detected by PIP 20.2.4 and below but it was there and
the version of ``Google BigQuery`` python client was not matching on both sides. As the result, when
both ``apache.beam`` and ``google`` provider were installed, some features of the ``BigQuery`` operators
might not work properly. This was cause by ``apache-beam`` client not yet supporting the new google
python clients when ``apache-beam[gcp]`` extra was used. The ``apache-beam[gcp]`` extra is used
by ``Dataflow`` operators and while they might work with the newer version of the ``Google BigQuery``
python client, it is not guaranteed.

This version introduces additional extra requirement for the ``apache.beam`` extra of the ``google`` provider
and symmetrically the additional requirement for the ``google`` extra of the ``apache.beam`` provider.
Both ``google`` and ``apache.beam`` provider do not use those extras by default, but you can specify
them when installing the providers. The consequence of that is that some functionality of the ``Dataflow``
operators might not be available.

Unfortunately the only ``complete`` solution to the problem is for the ``apache.beam`` to migrate to the
new (>=2.0.0) Google Python clients.

This is the extra for the ``google`` provider:

.. code-block:: python

        extras_require = (
            {
                # ...
                "apache.beam": ["apache-airflow-providers-apache-beam", "apache-beam[gcp]"],
                # ...
            },
        )

And likewise this is the extra for the ``apache.beam`` provider:

.. code-block:: python

        extras_require = ({"google": ["apache-airflow-providers-google", "apache-beam[gcp]"]},)

You can still run this with PIP version <= 20.2.4 and go back to the previous behaviour:

.. code-block:: shell

  pip install apache-airflow-providers-google[apache.beam]

or

.. code-block:: shell

  pip install apache-airflow-providers-apache-beam[google]

But be aware that some ``BigQuery`` operators functionality might not be available in this case.

1.0.1
.....

Bug fixes
~~~~~~~~~

* ``Improve Apache Beam operators - refactor operator - common Dataflow logic (#14094)``
* ``Corrections in docs and tools after releasing provider RCs (#14082)``
* ``Remove WARNINGs from BeamHook (#14554)``

1.0.0
.....

Initial version of the provider.
