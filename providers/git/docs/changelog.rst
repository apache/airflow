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


Changelog
---------

0.1.1
.....

Misc
~~~~

* ``Fix misleading error message when GitHook creation fails (#59084)``
* ``Add backcompat for exceptions in providers (#58727)``

Doc-only
~~~~~~~~

* ``Updates to release process of providers (#58316)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

0.1.0
.....

Features
~~~~~~~~

* ``Add HTTP URL authentication support to GitHook (#58194)``

Bug Fixes
~~~~~~~~~

* ``Remove '.git' folder from versions in GitDagBundle (#57069)``
* ``Fix Git corruption recovery by moving fetch into retry context (#56913)``

Misc
~~~~

* ``Convert all airflow distributions to be compliant with ASF requirements (#58138)``

Doc-only
~~~~~~~~

* ``Fixing some typos and spelling errors (#57186)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Delete all unnecessary LICENSE Files (#58191)``
   * ``Fix the '__init__.py' for tests (#58211)``
   * ``Enable PT006 rule to 6 files in providers (edge3,git) (#58018)``
   * ``Fix documentation/provider.yaml consistencies (#57283)``

0.0.9
.....

Bug Fixes
~~~~~~~~~

* ``Fix corrupted bare Git repository recovery in DAG bundles (#56206)``

Misc
~~~~

* ``Migrate git provider to ''common.compat'' (#57008)``

Doc-only
~~~~~~~~

* ``Remove placeholder Release Date in changelog and index files (#56056)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

0.0.8
.....


Bug Fixes
~~~~~~~~~

* ``Fix https access token repo url (#54643)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

0.0.7
.....


Bug Fixes
~~~~~~~~~

* ``Catch Exception in git hook instantiation (#55079)``
* ``Fix process leaks in 'GitDagBundle' repository management (#54997)``

Doc-only
~~~~~~~~

* ``Add missing changelog provider for Git (#54496)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix git connection test by adding required conn_type field (#54681)``
   * ``Replace API server's direct Connection access workaround in BaseHook (#54083)``
   * ``Switch pre-commit to prek (#54258)``
   * ``Fix Airflow 2 reference in README/index of providers (#55240)``

0.0.6
.....

Misc
~~~~

* ``Refactor bundle view_url to not instantiate bundle on server components (#52876)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

0.0.5
.....

Bug Fixes
~~~~~~~~~

* ``fix: always create GitHook even when 'repo_url' is provided (#52897)``

Misc
~~~~

* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Moving BaseHook usages to version_compat for git (#52944)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

0.0.4
.....

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Drop support for Python 3.9 (#52072)``

Doc-only
~~~~~~~~

* ``Minor pre-commit fixes (#51769)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove pytest.mark.db_test: Git (#52035)``
   * ``Introducing fixture to create 'Connections' without DB in provider tests (#51930)``

0.0.3
.....

Bug Fixes
~~~~~~~~~

* ``add user_name to http git (#51256)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare release for providers May 2025 (#50531)``
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``

0.0.2
.....

Bug Fixes
~~~~~~~~~

* ``Don't log repo_url in git dag bundle (#48909)``
* ``Make git connection optional for git dag bundle (#49270)``
* ``Use 'git_default' if the user defines nothing (#49359)``

Misc
~~~~

* ``Include subdir in Gitbundle view url (#49239)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Apr 3rd wave of providers (#49338)``
   * ``Update documentation for edge3 and git provider (#49365)``

0.0.1
.....

.. note::
  Provider is still WIP. It can be used with production but we may introduce breaking changes without following semver until version 1.0.0

* ``Initial version of git provider (#47636)``
