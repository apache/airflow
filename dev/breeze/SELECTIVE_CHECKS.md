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

## Groups of files that selective check make decisions on

We have the following Groups of files for CI that determine which tests are run:

* `Environment files` - if any of those changes, that forces 'full tests needed' mode, because changes
  there might simply change the whole environment of what is going on in CI (Container image, dependencies)
* `Python production files` and `Javascript production files` - this area is useful in CodeQL Security scanning
  - if any of the python or javascript files for airflow "production" changed, this means that the security
  scans should run
* `Always test files` - Files that belong to "Always" run tests.
* `API tests files` and `Codegen test files` - those are OpenAPI definition files that impact
  Open API specification and determine that we should run dedicated API tests.
* `Helm files` - change in those files impacts helm "rendering" tests - `chart` folder and `helm_tests` folder.
* `Setup files` - change in the setup files indicates that we should run  `upgrade to newer dependencies` -
  pyproject.toml and  generated dependencies files in `generated` folder. The dependency files and part of
  the pyproject.toml are automatically generated from the provider.yaml files in provider by
  the `update-providers-dependencies` pre-commit. The provider.yaml is a single source of truth for each
  provider.
* `DOC files` - change in those files indicate that we should run documentation builds (both airflow sources
   and airflow documentation)
* `WWW files` - those are files for the WWW part of our UI (useful to determine if UI tests should run)
* `System test files` - those are the files that are part of system tests (system tests are not automatically
  run in our CI, but Airflow stakeholders are running the tests and expose dashboards for them at
  [System Test Dashbards](https://airflow.apache.org/ecosystem/#airflow-provider-system-test-dashboards)
* `Kubernetes files` - determine if any of Kubernetes related tests should be run
* `All Python files` - if none of the Python file changed, that indicates that we should not run unit tests
* `All source files` - if none of the sources change, that indicates that we should probably not build
  an image and run any image-based static checks
* `All Airflow Python files` - files that are checked by `mypy-core` static checks
* `All Providers Python files` - files that are checked by `mypy-providers` static checks
* `All Dev Python files` - files that are checked by `mypy-dev` static checks
* `All Docs Python files` - files that are checked by `mypy-docs` static checks
* `All Provider Yaml files` - all provider yaml files


We have a number of `TEST_TYPES` that can be selectively disabled/enabled based on the
content of the incoming PR. Usually they are limited to a sub-folder of the "tests" folder but there
are some exceptions. You can read more about those in `testing.rst <contribiting-docs/09_testing.rst>`. Those types
are determined by selective checks and are used to run `DB` and `Non-DB` tests.

The `DB` tests inside each `TEST_TYPE` are run sequentially (because they use DB as state) while `TEST_TYPES`
are run in parallel - each within separate docker-compose project. The `Non-DB` tests are all executed
together using `pytest-xdist` (pytest-xdist distributes the tests among parallel workers).

## Selective check decision rules

* `Full tests` case is enabled when the event is PUSH, or SCHEDULE or we miss commit info or any of the
  important environment files (`pyproject.toml`, `Dockerfile`, `scripts`,
  `generated/provider_dependencies.json` etc.) changed or  when `full tests needed` label is set.
  That enables all matrix combinations of variables (representative) and all possible test type. No further
  checks are performed. See also [1] note below.
* Python, Kubernetes, Backend, Kind, Helm versions are limited to "defaults" only unless `Full tests` mode
  is enabled.
* `Python scans`, `Javascript scans`, `API tests/codegen`, `UI`, `WWW`, `Kubernetes` tests and `DOC builds`
  are enabled if any of the relevant files have been changed.
* `Helm` tests are run only if relevant files have been changed and if current branch is `main`.
* If no Source files are changed - no tests are run and no further rules below are checked.
* `CI Image building` is enabled if either test are run, docs are build.
* `PROD Image building` is enabled when kubernetes tests are run.
* In case of `Providers` test in regular PRs, additional check is done in order to determine which
  providers are affected and the actual selection is made based on that:
  * if directly provider code is changed (either in the provider, test or system tests) then this provider
    is selected.
  * if there are any providers that depend on the affected providers, they are also included in the list
    of affected providers (but not recursively - only direct dependencies are added)
  * if there are any changes to "common" provider code not belonging to any provider (usually system tests
    or tests), then tests for all Providers are run
* The specific unit test type is enabled only if changed files match the expected patterns for each type
  (`API`, `CLI`, `WWW`, `Providers`, `Operators` etc.). The `Always` test type is added always if any unit
  tests are run. `Providers` tests are removed if current branch is different than `main`
* If there are no files left in sources after matching the test types and Kubernetes files,
  then apparently some Core/Other files have been changed. This automatically adds all test
  types to execute. This is done because changes in core might impact all the other test types.
* if `CI Image building` is disabled, only basic pre-commits are enabled - no 'image-depending` pre-commits
  are enabled.
* If there are some setup files changed, `upgrade to newer dependencies` is enabled.
* If docs are build, the `docs-list-as-string` will determine which docs packages to build. This is based on
  several criteria: if any of the airflow core, charts, docker-stack, providers files or docs have changed,
  then corresponding packages are build (including cross-dependent providers). If any of the core files
  changed, also providers docs are built because all providers depend on airflow docs. If any of the docs
  build python files changed or when build is "canary" type in main - all docs packages are built.

## Skipping pre-commits (Static checks)

Our CI always run pre-commit checks with `--all-files` flag. This is in order to avoid cases where
different check results are run when only subset of files is used. This has an effect that the pre-commit
tests take a long time to run when all of them are run. Selective checks allow to save a lot of time
for those tests in regular PRs of contributors by smart detection of which pre-commits should be skipped
when some files are not changed. Those are the rules implemented:

* The `identity` check is always skipped (saves space to display all changed files in CI)
* The provider specific checks are skipped when builds are running in v2_* branches (we do not build
  providers from those branches. Those are the checks skipped in this case:
  * check-airflow-provider-compatibility
  * check-extra-packages-references
  * check-provider-yaml-valid
  * lint-helm-chart
  * mypy-providers
* If "full tests" mode is detected, no more pre-commits are skipped - we run all of them
* The following checks are skipped if those files are not changed:
  * if no `All Providers Python files` changed - `mypy-providers` check is skipped
  * if no `All Airflow Python files` changed - `mypy-core` check is skipped
  * if no `All Docs Python files` changed - `mypy-docs` check is skipped
  * if no `All Dev Python files` changed - `mypy-dev` check is skipped
  * if no `WWW files` changed - `ts-compile-format-lint-www` check is skipped
  * if no `All Python files` changed - `flynt` check is skipped
  * if no `Helm files` changed - `lint-helm-chart` check is skipped
  * if no `All Providers Python files` and no `All Providers Yaml files` are changed -
    `check-provider-yaml-valid` check is skipped

## Suspended providers

The selective checks will fail in PR if it contains changes to a suspended provider unless you set the
label `allow suspended provider changes` in the PR. This is to prevent accidental changes to suspended
providers.


## Selective check outputs

The selective check outputs available are described below. In case of `list-as-string` values,
empty string means `everything`, where lack of the output means `nothing` and list elements are
separated by spaces. This is to accommodate for the wau how outputs of this kind can be easily used by
Github Actions to pass the list of parameters to a command to execute


| Output                             | Meaning of the output                                                                                   | Example value                                       | List as string |
|------------------------------------|---------------------------------------------------------------------------------------------------------|-----------------------------------------------------|----------------|
| affected-providers-list-as-string  | List of providers affected when they are selectively affected.                                          | airbyte http                                        | *              |
| all-python-versions                | List of all python versions there are available in the form of JSON array                               | ['3.8', '3.9', '3.10']                              |                |
| all-python-versions-list-as-string | List of all python versions there are available in the form of space separated string                   | 3.8 3.9 3.10                                        | *              |
| basic-checks-only                  | Whether to run all static checks ("false") or only basic set of static checks ("true")                  | false                                               |                |
| cache-directive                    | Which cache should be used for images ("registry", "local" , "disabled")                                | registry                                            |                |
| debug-resources                    | Whether resources usage should be printed during parallel job execution ("true"/ "false")               | false                                               |                |
| default-branch                     | Which branch is default for the build ("main" for main branch, "v2-4-test" for 2.4 line etc.)           | main                                                |                |
| default-constraints-branch         | Which branch is default for the build ("constraints-main" for main branch, "constraints-2-4" etc.)      | constraints-main                                    |                |
| default-helm-version               | Which Helm version to use as default                                                                    | v3.9.4                                              |                |
| default-kind-version               | Which Kind version to use as default                                                                    | v0.16.0                                             |                |
| default-kubernetes-version         | Which Kubernetes version to use as default                                                              | v1.25.2                                             |                |
| default-mysql-version              | Which MySQL version to use as default                                                                   | 5.7                                                 |                |
| default-postgres-version           | Which Postgres version to use as default                                                                | 10                                                  |                |
| default-python-version             | Which Python version to use as default                                                                  | 3.8                                                 |                |
| docs-build                         | Whether to build documentation ("true"/"false")                                                         | true                                                |                |
| docs-list-as-string                | What filter to apply to docs building - based on which documentation packages should be built           | apache-airflow helm-chart google                    |                |
| full-tests-needed                  | Whether this build runs complete set of tests or only subset (for faster PR builds)                     | false                                               |                |
| helm-version                       | Which Helm version to use for tests                                                                     | v3.9.4                                              |                |
| ci-image-build                     | Whether CI image build is needed                                                                        | true                                                |                |
| prod-image-build                   | Whether PROD image build is needed                                                                      | true                                                |                |
| kind-version                       | Which Kind version to use for tests                                                                     | v0.16.0                                             |                |
| kubernetes-combos-list-as-string   | All combinations of Python version and Kubernetes version to use for tests as space-separated string    | 3.8-v1.25.2 3.9-v1.26.4                             | *              |
| kubernetes-versions                | All Kubernetes versions to use for tests as JSON array                                                  | ['v1.25.2']                                         |                |
| kubernetes-versions-list-as-string | All Kubernetes versions to use for tests as space-separated string                                      | v1.25.2                                             | *              |
| mysql-exclude                      | Which versions of MySQL to exclude for tests as JSON array                                              | []                                                  |                |
| mysql-versions                     | Which versions of MySQL to use for tests as JSON array                                                  | ['5.7']                                             |                |
| needs-api-codegen                  | Whether "api-codegen" are needed to run ("true"/"false")                                                | true                                                |                |
| needs-api-tests                    | Whether "api-tests" are needed to run ("true"/"false")                                                  | true                                                |                |
| needs-helm-tests                   | Whether Helm tests are needed to run ("true"/"false")                                                   | true                                                |                |
| needs-javascript-scans             | Whether javascript CodeQL scans should be run ("true"/"false")                                          | true                                                |                |
| needs-python-scans                 | Whether Python CodeQL scans should be run ("true"/"false")                                              | true                                                |                |
| parallel-test-types-list-as-string | Which test types should be run for unit tests                                                           | API Always Providers\[amazon\] Providers\[-amazon\] | *              |
| postgres-exclude                   | Which versions of Postgres to exclude for tests as JSON array                                           | []                                                  |                |
| postgres-versions                  | Which versions of Postgres to use for tests as JSON array                                               | ['10']                                              |                |
| python-versions                    | Which versions of Python to use for tests as JSON array                                                 | ['3.8']                                             |                |
| python-versions-list-as-string     | Which versions of MySQL to use for tests as space-separated string                                      | 3.8                                                 | *              |
| run-kubernetes-tests               | Whether Kubernetes tests should be run ("true"/"false")                                                 | true                                                |                |
| run-tests                          | Whether unit tests should be run ("true"/"false")                                                       | true                                                |                |
| run-www-tests                      | Whether WWW tests should be run ("true"/"false")                                                        | true                                                |                |
| skip-pre-commits                   | Which pre-commits should be skipped during the static-checks run                                        | true                                                |                |
| skip-provider-tests                | When provider tests should be skipped (on non-main branch or when no provider changes detected)         | true                                                |                |
| sqlite-exclude                     | Which versions of Sqlite to exclude for tests as JSON array                                             | []                                                  |                |
| upgrade-to-newer-dependencies      | Whether the image build should attempt to upgrade all dependencies (might be true/false or commit hash) | false                                               |                |


[1] Note for deciding if `full tests needed` mode is enabled and provider.yaml files.

When we decided whether to run `full tests` we do not check (directly) if provider.yaml files changed,
even if they are single source of truth for provider dependencies and when you add a dependency there,
the environment changes and generally full tests are advised.

This is because provider.yaml change will automatically trigger (via `update-provider-dependencies` pre-commit)
generation of `generated/provider_dependencies.json` and `pyproject.toml` gets updated as well. This is a far
better indication if we need to run full tests than just checking if provider.yaml files changed, because
provider.yaml files contain more information than just dependencies - they are the single source of truth
for a lot of information for each provider and sometimes (for example when we update provider documentation
or when new Hook class is added), we do not need to run full tests.

That's why we do not base our `full tests needed` decision on changes in dependency files that are generated
from the `provider.yaml` files.
