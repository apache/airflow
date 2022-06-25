<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Selective CI Checks](#selective-ci-checks)

# Selective CI Checks

In order to optimise our CI jobs, we've implemented optimisations to only run selected checks for some
kind of changes. The logic implemented reflects the internal architecture of Airflow 2.0 packages,
and it helps to keep down both the usage of jobs in GitHub Actions and CI feedback time to
contributors in case of simpler changes.

We have the following Groups of files for CI that determine which tests are run:

* `Environment files` - if any of those changes, that forces 'run everything' mode, because changes there might
  simply change the whole environment of what is going on in CI (Container image, dependencies)
* `Python and Javascript production files` - this area is useful in CodeQL Security scanning - if any of
  the python or javascript files for airflow "production" changed, this means that the security scans should run
* `API tests and codegen files` - those are OpenAPI definition files that impact Open API specification and
  determine that we should run dedicated API tests.
* `Helm files` - change in those files impacts helm "rendering" tests
* `Setup files` - change in the setup files indicates that we should run  `upgrade to newer dependencies`
* `DOCs files` - change in those files indicate that we should run documentation builds
* `UI and WWW files` - those are files for the UI and WWW part of our UI (useful to determine if UI
  tests should run)
* `Kubernetes files` - determine if any of Kubernetes related tests should be run
* `All Python files` - if none of the Python file changed, that indicates that we should not run unit tests
* `All source files` - if none of the sources change, that indicates that we should probably not build
  an image and run any image-based static checks

We have the following unit test types that can be selectively disabled/enabled based on the
content of the incoming PR:

* Always - those are tests that should be always executed (always folder)
* Core - for the core Airflow functionality (core folder)
* API - Tests for the Airflow API (api and api_connexion folders)
* CLI - Tests for the Airflow CLI (cli folder)
* WWW - Tests for the Airflow webserver (www folder)
* Providers - Tests for all Providers of Airflow (providers folder)

We also have several special kinds of tests that are not separated by packages, but they are marked with
pytest markers. They can be found in any of those packages and they can be selected by the appropriate
pytest custom command line options. See `TESTING.rst <TESTING.rst>`_ for details but those are:

* Integration - tests that require external integration images running in docker-compose
* Quarantined - tests that are flaky and need to be fixed
* Postgres - tests that require Postgres database. They are only run when backend is Postgres
* MySQL - tests that require MySQL database. They are only run when backend is MySQL

Even if the types are separated, In case they share the same backend version/python version, they are
run sequentially in the same job, on the same CI machine. Each of them in a separate `docker run` command
and with additional docker cleaning between the steps to not fall into the trap of exceeding resource
usage in one big test run, but also not to increase the number of jobs per each Pull Request.

The logic implements the following rules:

* `Full tests` mode is enabled when the event is PUSH, or SCHEDULE or when "full tests needed" label is set.
  That enables all matrix combinations of variables, and all possible tests
* Python, Kubernetes, Backend, Kind, Helm versions are limited to "defaults" only unless `Full tests` mode
  is enabled.
* If "Commit" to work on cannot be determined, or `Full Test` mode is enabled or some of the important
  environment files (setup.py, setup.cfg, Dockerfile, build scripts) changed - all unit tests are
  executed - this is `run everything` mode. No further checks are performed.
* `Python scans`, `Javascript scans`, `API tests/codegen`, `UI`, `WWW`, `Kubernetes` tests and `DOC builds`
  are enabled if any of the relevant files have been changed.
* `Helm` tests are run only if relevant files have been changed and if current branch is `main`.
* If no Source files are changed - no tests are run and no further rules below are checked.
* `Image building` is enabled if either test are run, docs are build or kubernetes tests are run. All those
  need `CI` or `PROD` images to be built.
* The specific unit test type is enabled only if changed files match the expected patterns for each type
  (`API`, `CLI`, `WWW`, `Providers`). The `Always` test type is added always if any unit tests are run.
  `Providers` tests are removed if current branch is different than `main`
* If there are no files left in sources after matching the test types and Kubernetes files,
  then apparently some Core/Other files have been changed. This automatically adds all test
  types to execute. This is done because changes in core might impact all the other test types.
* if `Image building` is disabled, only basic pre-commits are enabled - no 'image-depending` pre-commits
  are enabled.
* If there are some setup files changed, `upgrade to newer dependencies` is enabled.
