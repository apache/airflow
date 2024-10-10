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
  - [Groups of files that selective check make decisions on](#groups-of-files-that-selective-check-make-decisions-on)
  - [Selective check decision rules](#selective-check-decision-rules)
  - [Skipping pre-commits (Static checks)](#skipping-pre-commits-static-checks)
  - [Suspended providers](#suspended-providers)
  - [Selective check outputs](#selective-check-outputs)
  - [Committer vs. Non-committer PRs](#committer-vs-non-committer-prs)
  - [Changing behaviours of the CI runs by setting labels](#changing-behaviours-of-the-ci-runs-by-setting-labels)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

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
* `Build files` - change in the files indicates that we should run  `upgrade to newer dependencies` -
  build dependencies in `pyproject.toml` and  generated dependencies files in `generated` folder.
  The dependencies are automatically generated from the `provider.yaml` files in provider by
  the `hatch_build.py` build hook. The provider.yaml is a single source of truth for each
  provider and `hatch_build.py` for all regular dependencies.
* `DOC files` - change in those files indicate that we should run documentation builds (both airflow sources
  and airflow documentation)
* `UI files` - those are files for the new full React UI (useful to determine if UI tests should run)
* `WWW files` - those are files for the WWW part of our UI (useful to determine if UI tests should run)
* `System test files` - those are the files that are part of system tests (system tests are not automatically
  run in our CI, but Airflow stakeholders are running the tests and expose dashboards for them at
  [System Test Dashbards](https://airflow.apache.org/ecosystem/#airflow-provider-system-test-dashboards)
* `Kubernetes files` - determine if any of Kubernetes related tests should be run
* `All Python files` - if none of the Python file changed, that indicates that we should not run unit tests
* `All source files` - if none of the sources change, that indicates that we should probably not build
  an image and run any image-based static checks
* `All Airflow Python files` - files that are checked by `mypy-airflow` static checks
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
* If there are some build dependencies changed (`hatch_build.py` and updated system dependencies in
  the `pyproject.toml` - then `upgrade to newer dependencies` is enabled.
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
  * if no `All Airflow Python files` changed - `mypy-airflow` check is skipped
  * if no `All Docs Python files` changed - `mypy-docs` check is skipped
  * if no `All Dev Python files` changed - `mypy-dev` check is skipped
  * if no `UI files` changed - `ts-compile-format-lint-ui` check is skipped
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


| Output                                 | Meaning of the output                                                                                | Example value                             | List as string |
|----------------------------------------|------------------------------------------------------------------------------------------------------|-------------------------------------------|----------------|
| affected-providers-list-as-string      | List of providers affected when they are selectively affected.                                       | airbyte http                              | *              |
| all-python-versions                    | List of all python versions there are available in the form of JSON array                            | ['3.9', '3.10']                           |                |
| all-python-versions-list-as-string     | List of all python versions there are available in the form of space separated string                | 3.9 3.10                                  | *              |
| all-versions                           | If set to true, then all python, k8s, DB versions are used for tests.                                | false                                     |                |
| basic-checks-only                      | Whether to run all static checks ("false") or only basic set of static checks ("true")               | false                                     |                |
| build_system_changed_in_pyproject_toml | When builds system dependencies changed in pyproject.toml changed in the PR.                         | false                                     |                |
| chicken-egg-providers                  | List of providers that should be considered as "chicken-egg" - expecting development Airflow version |                                           |                |
| ci-image-build                         | Whether CI image build is needed                                                                     | true                                      |                |
| debug-resources                        | Whether resources usage should be printed during parallel job execution ("true"/ "false")            | false                                     |                |
| default-branch                         | Which branch is default for the build ("main" for main branch, "v2-4-test" for 2.4 line etc.)        | main                                      |                |
| default-constraints-branch             | Which branch is default for the build ("constraints-main" for main branch, "constraints-2-4" etc.)   | constraints-main                          |                |
| default-helm-version                   | Which Helm version to use as default                                                                 | v3.9.4                                    |                |
| default-kind-version                   | Which Kind version to use as default                                                                 | v0.16.0                                   |                |
| default-kubernetes-version             | Which Kubernetes version to use as default                                                           | v1.25.2                                   |                |
| default-mysql-version                  | Which MySQL version to use as default                                                                | 5.7                                       |                |
| default-postgres-version               | Which Postgres version to use as default                                                             | 10                                        |                |
| default-python-version                 | Which Python version to use as default                                                               | 3.9                                       |                |
| docker-cache                           | Which cache should be used for images ("registry", "local" , "disabled")                             | registry                                  |                |
| docs-build                             | Whether to build documentation ("true"/"false")                                                      | true                                      |                |
| docs-list-as-string                    | What filter to apply to docs building - based on which documentation packages should be built        | apache-airflow helm-chart google          |                |
| full-tests-needed                      | Whether this build runs complete set of tests or only subset (for faster PR builds) [1]              | false                                     |                |
| generated-dependencies-changed         | Whether generated dependencies have changed ("true"/"false")                                         | false                                     |                |
| hatch-build-changed                    | When hatch build.py changed in the PR.                                                               | false                                     |                |
| helm-version                           | Which Helm version to use for tests                                                                  | v3.9.4                                    |                |
| is-airflow-runner                      | Whether runner used is an airflow or infrastructure runner (true if airflow/false if infrastructure) | false                                     |                |
| is-amd-runner                          | Whether runner used is an AMD one                                                                    | true                                      |                |
| is-arm-runner                          | Whether runner used is an ARM one                                                                    | false                                     |                |
| is-committer-build                     | Whether the build is triggered by a committer                                                        | false                                     |                |
| is-k8s-runner                          | Whether the build runs on our k8s infrastructure                                                     | false                                     |                |
| is-self-hosted-runner                  | Whether the runner is self-hosted                                                                    | false                                     |                |
| is-vm-runner                           | Whether the runner uses VM to run                                                                    | true                                      |                |
| kind-version                           | Which Kind version to use for tests                                                                  | v0.16.0                                   |                |
| kubernetes-combos-list-as-string       | All combinations of Python version and Kubernetes version to use for tests as space-separated string | 3.9-v1.25.2 3.9-v1.26.4                   | *              |
| kubernetes-versions                    | All Kubernetes versions to use for tests as JSON array                                               | ['v1.25.2']                               |                |
| kubernetes-versions-list-as-string     | All Kubernetes versions to use for tests as space-separated string                                   | v1.25.2                                   | *              |
| mypy-folders                           | List of folders to be considered for mypy                                                            | []                                        |                |
| mysql-exclude                          | Which versions of MySQL to exclude for tests as JSON array                                           | []                                        |                |
| mysql-versions                         | Which versions of MySQL to use for tests as JSON array                                               | ['5.7']                                   |                |
| needs-api-codegen                      | Whether "api-codegen" are needed to run ("true"/"false")                                             | true                                      |                |
| needs-api-tests                        | Whether "api-tests" are needed to run ("true"/"false")                                               | true                                      |                |
| needs-helm-tests                       | Whether Helm tests are needed to run ("true"/"false")                                                | true                                      |                |
| needs-javascript-scans                 | Whether javascript CodeQL scans should be run ("true"/"false")                                       | true                                      |                |
| needs-mypy                             | Whether mypy check is supposed to run in this build                                                  | true                                      |                |
| needs-python-scans                     | Whether Python CodeQL scans should be run ("true"/"false")                                           | true                                      |                |
| parallel-test-types-list-as-string     | Which test types should be run for unit tests                                                        | API Always Providers Providers\[-google\] | *              |
| postgres-exclude                       | Which versions of Postgres to exclude for tests as JSON array                                        | []                                        |                |
| postgres-versions                      | Which versions of Postgres to use for tests as JSON array                                            | ['10']                                    |                |
| prod-image-build                       | Whether PROD image build is needed                                                                   | true                                      |                |
| prod-image-build                       | Whether PROD image build is needed                                                                   | true                                      |                |
| providers-compatibility-checks         | List of dicts: (python_version, airflow_version, removed_providers) for compatibility checks         | []                                        |                |
| pyproject-toml-changed                 | When pyproject.toml changed in the PR.                                                               | false                                     |                |
| python-versions                        | List of python versions to use for that build                                                        | ['3.9']                                   | *              |
| python-versions-list-as-string         | Which versions of MySQL to use for tests as space-separated string                                   | 3.9                                       | *              |
| run-amazon-tests                       | Whether Amazon tests should be run ("true"/"false")                                                  | true                                      |                |
| run-kubernetes-tests                   | Whether Kubernetes tests should be run ("true"/"false")                                              | true                                      |                |
| run-tests                              | Whether unit tests should be run ("true"/"false")                                                    | true                                      |                |
| run-ui-tests                           | Whether WWW tests should be run ("true"/"false")                                                     | true                                      |                |
| run-www-tests                          | Whether WWW tests should be run ("true"/"false")                                                     | true                                      |                |
| runs-on-as-json-default                | List of labels assigned for runners for that build for default runs for that build (as string)       | ["ubuntu-22.04"]                          |                |
| runs-on-as-json-self-hosted            | List of labels assigned for runners for that build for self hosted runners                           | ["self-hosted", "Linux", "X64"]           |                |
| runs-on-as-json-public                 | List of labels assigned for runners for that build for public runners                                | ["ubuntu-22.04"]                          |                |
| skip-pre-commits                       | Which pre-commits should be skipped during the static-checks run                                     | check-provider-yaml-valid,flynt,identity  |                |
| skip-provider-tests                    | When provider tests should be skipped (on non-main branch or when no provider changes detected)      | true                                      |                |
| sqlite-exclude                         | Which versions of Sqlite to exclude for tests as JSON array                                          | []                                        |                |
| testable-integrations                  | List of integrations that are testable in the build as JSON array                                    | ['mongo', 'kafka', 'mssql']               |                |
| upgrade-to-newer-dependencies          | Whether the image build should attempt to upgrade all dependencies (true/false or commit hash)       | false                                     |                |


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
from the `provider.yaml` files, but on `generated/provider_dependencies.json` and `pyproject.toml` files being
modified. This can be overridden by setting `full tests needed` label in the PR.

## Committer vs. Non-committer PRs

There is a difference in how the CI jobs are run for committer and non-committer PRs from forks.
The main reason is security; we do not want to run untrusted code on our infrastructure for self-hosted runners.
Additionally, we do not want to run unverified code during the `Build imaage` workflow, as that workflow has
access to the `GITHUB_TOKEN`, which can write to our Github Registry (used to cache
images between runs). These images are built on self-hosted runners, and we must ensure that
those runners are not misused, such as for mining cryptocurrencies on behalf of the person who opened the
pull request from their newly created fork of Airflow.

This is why the `Build Images` workflow checks whether the actor of the PR (`GITHUB_ACTOR`) is one of the committers.
If not, the workflows and scripts used to run image building come only from the ``target`` branch
of the repository, where these scripts have been reviewed and approved by committers before being merged. This is controlled by the selective checks that set `is-committer-build` to `true` in
the build-info job of the workflow to determine if the actor is in the committers'
list. This setting can be overridden by the `non-committer build` label in the PR.

Also, for most of the jobs, committer builds use "Self-hosted" runners by default, while non-committer
builds use "Public" runners. For committers, this can be overridden by setting the
`use public runners` label in the PR.

## Changing behaviours of the CI runs by setting labels

Also, currently for most of the jobs, committer builds by default use "Self-hosted" runners, while
non-committer builds use "Public" runners. For committers, this can be overridden by setting the
`use public runners` label in the PR. In the future when we might also switch committers to public runners.
Committers will be able to use `use self-hosted runners` label in the PR to force using self-hosted runners.
The `use public runners` label will still be available for committers and they will be able to set it for
builds that also have `canary` label set to also switch the `canary` builds to public runners.

If you are testing CI workflow changes and want to test it for more complete matrix combinations generated by
the jobs - you can set `all versions` label in the PR. This will run the PRs with the same combinations
of versions as the `canary` main build. Using `all versions` is automatically set when build dependencies
change in `pyproject.toml` or when dependencies change for providers in `generated/provider_dependencies.json`
or when `hatch_build.py` changes.

If you are running an `apache` PR, you can also set `canary` label for such PR and in this case, all the
`canary` properties of build will be used: `self-hosted` runners, `full tests needed` mode, `all versions`
as well as all canary-specific jobs will run there. You can modify this behaviour of the `canary` run by
applying `use public runners`, and `default versions only` labels to the PR as well which will still run
a `canary` equivalent build but with public runners an default Python/K8S versions only - respectively.

If you are testing CI workflow changes and change `pyproject.toml` or `generated/provider_dependencies.json`
and you want to limit the number of matrix combinations generated by
the jobs - you can set `default versions only` label in the PR. This will limit the number of versions
used in the matrix to the default ones (default Python version and default Kubernetes version).

If you are testing CI workflow changes and want to limit the number of matrix combinations generated by
the jobs - you can also set `latest versions only` label in the PR. This will limit the number of versions
used in the matrix to the latest ones (latest Python version and latest Kubernetes version).

You can also disable cache if you want to make sure your tests will run with image that does not have
left-over package installed from the past cached image - by setting `disable image cache` label in the PR.

By default, all outputs of successful parallel tests are not shown. You can enable them by setting
`include success outputs` label in the PR. This makes the logs of mostly successful tests a lot longer
and more difficult to sift through, but it might be useful in case you want to compare successful and
unsuccessful runs of the tests.

This table summarizes the labels you can use on PRs to control the selective checks and the CI runs:

| Label                            | Affected outputs                 | Meaning                                                                                   |
|----------------------------------|----------------------------------|-------------------------------------------------------------------------------------------|
| all versions                     | all-versions, *-versions-*       | Run tests for all python and k8s versions.                                                |
| allow suspended provider changes | allow-suspended-provider-changes | Allow changes to suspended providers.                                                     |
| canary                           | is-canary-run                    | If set, the PR run from apache/airflow repo behaves as `canary` run.                      |
| debug ci resources               | debug-ci-resources               | If set, then debugging resources is enabled during parallel tests and you can see them.   |
| default versions only            | all-versions, *-versions-*       | If set, the number of Python and Kubernetes, DB versions are limited to the default ones. |
| disable image cache              | docker-cache                     | If set, the image cache is disables when building the image.                              |
| full tests needed                | full-tests-needed                | If set, complete set of tests are run                                                     |
| include success outputs          | include-success-outputs          | If set, outputs of successful parallel tests are shown not only failed outputs.           |
| latest versions only             | *-versions-*, *-versions-*       | If set, the number of Python, Kubernetes, DB versions will be limited to the latest ones. |
| non committer build              | is-committer-build               | If set, the scripts used for images are used from target branch for committers.           |
| upgrade to newer dependencies    | upgrade-to-newer-dependencies    | If set to true (default false) then dependencies in the CI image build are upgraded.      |
| use public runners               | runs-on-as-json-default          | Force using public runners as default runners.                                            |
| use self-hosted runners          | runs-on-as-json-default          | Force using self-hosted runners as default runners.                                       |

-----

Read next about [Workflows](05_workflows.md)
