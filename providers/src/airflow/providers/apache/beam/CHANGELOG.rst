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
   * ``Use reproducible builds for provider packages (#35693)``
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

.. Review and move the new changes to one of the sections above:
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
