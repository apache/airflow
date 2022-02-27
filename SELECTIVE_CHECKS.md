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

# Selective CI Checks

In order to optimise our CI jobs, we've implemented optimisations to only run selected checks for some
kind of changes. The logic implemented reflects the internal architecture of Airflow 2.0 packages
and it helps to keep down both the usage of jobs in GitHub Actions as well as CI feedback time to
contributors in case of simpler changes.

We have the following test types (separated by packages in which they are):

* Always - those are tests that should be always executed (always folder)
* Core - for the core Airflow functionality (core folder)
* API - Tests for the Airflow API (api and api_connexion folders)
* CLI - Tests for the Airflow CLI (cli folder)
* WWW - Tests for the Airflow webserver (www folder)
* Providers - Tests for all Providers of Airflow (providers folder)
* Other - all other tests (all other folders that are not part of any of the above)

We also have several special kinds of tests that are not separated by packages but they are marked with
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

The logic implemented for the changes works as follows:

1) In case of direct push (so when PR gets merged) or scheduled run, we always run all tests and checks.
   This is in order to make sure that the merge did not miss anything important. The remainder of the logic
   is executed only in case of Pull Requests. We do not add providers tests in case DEFAULT_BRANCH is
   different than main, because providers are only important in main branch and PRs to main branch.

2) We retrieve which files have changed in the incoming Merge Commit (github.sha is a merge commit
   automatically prepared by GitHub in case of Pull Request, so we can retrieve the list of changed
   files from that commit directly).

3) If any of the important, environment files changed (Dockerfile, ci scripts, setup.py, GitHub workflow
   files), then we again run all tests and checks. Those are cases where the logic of the checks changed
   or the environment for the checks changed so we want to make sure to check everything. We do not add
   providers tests in case DEFAULT_BRANCH is different than main, because providers are only
   important in main branch and PRs to main branch.

4) If any of py files changed: we need to have CI image and run full static checks so we enable image building

5) If any of docs changed: we need to have CI image so we enable image building

6) If any of chart files changed, we need to run helm tests so we enable helm unit tests

7) If any of API files changed, we need to run API tests so we enable them

8) If any of the relevant source files that trigger the tests have changed at all. Those are airflow
   sources, chart, tests and kubernetes_tests. If any of those files changed, we enable tests and we
   enable image building, because the CI images are needed to run tests.

9) Then we determine which types of the tests should be run. We count all the changed files in the
   relevant airflow sources (airflow, chart, tests, kubernetes_tests) first and then we count how many
   files changed in different packages:

    * in any case tests in `Always` folder are run. Those are special tests that should be run any time
       modifications to any Python code occurs. Example test of this type is verifying proper structure of
       the project including proper naming of all files.
    * if any of the Airflow API files changed we enable `API` test type
    * if any of the Airflow CLI files changed we enable `CLI` test type and Kubernetes tests (the
        K8S tests depend on CLI changes as helm chart uses CLI to run Airflow).
    * if this is a main branch and if any of the Provider files changed we enable `Providers` test type
    * if any of the WWW files changed we enable `WWW` test type
    * if any of the Kubernetes files changed we enable `Kubernetes` test type
    * Then we subtract count of all the `specific` above per-type changed files from the count of
      all changed files. In case there are any files changed, then we assume that some unknown files
      changed (likely from the core of airflow) and in this case we enable all test types above and the
      Core test types - simply because we do not want to risk to miss anything.
    * In all cases where tests are enabled we also add Integration and - depending on
      the backend used = Postgres or MySQL types of tests.

10) Quarantined tests are always run when tests are run - we need to run them often to observe how
    often they fail so that we can decide to move them out of quarantine. Details about the
    Quarantined tests are described in `TESTING.rst <TESTING.rst>`_

11) There is a special case of static checks. In case the above logic determines that the CI image
    needs to be built, we run long and more comprehensive version of static checks - including
    Mypy, Flake8. And those tests are run on all files, no matter how many files changed.
    In case the image is not built, we run only simpler set of changes - the longer static checks
    that require CI image are skipped, and we only run the tests on the files that changed in the incoming
    commit - unlike flake8/mypy, those static checks are per-file based and they should not miss any
    important change.

Similarly to selective tests we also run selective security scans. In Pull requests,
the Python scan will only run when there is a python code change and JavaScript scan will only run if
there is a JavaScript or `yarn.lock` file change. For main builds, all scans are always executed.

The selective check algorithm is shown here:


````mermaid
flowchart TD
A(PR arrives)-->B[Selective Check]
B-->C{Direct push merge?}
C-->|Yes| N[Enable images]
N-->D(Run Full Test<br />+Quarantined<br />Run full static checks)
C-->|No| E[Retrieve changed files]
E-->F{Environment files changed?}
F-->|Yes| N
F-->|No| G{Docs changed}
G-->|Yes| O[Enable images building]
O-->I{Chart files changed?}
G-->|No| I
I-->|Yes| P[Enable helm tests]
P-->J{API files changed}
I-->|No| J
J-->|Yes| Q[Enable API tests]
Q-->H{Sources changed?}
J-->|No| H
H-->|Yes| R[Enable Pytests]
R-->K[Determine test type]
K-->S{Core files changed}
S-->|Yes| N
S-->|No| M(Run selected test+<br />Integration, Quarantined<br />Full static checks)
H-->|No| L[Skip running test<br />Run subset of static checks]
```
