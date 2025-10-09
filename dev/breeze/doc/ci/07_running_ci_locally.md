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

- [Running the CI Jobs locally](#running-the-ci-jobs-locally)
- [Getting the CI image from failing job](#getting-the-ci-image-from-failing-job)
- [Options and environment variables used](#options-and-environment-variables-used)
  - [Basic variables](#basic-variables)
  - [Test variables](#test-variables)
  - [In-container environment initialization](#in-container-environment-initialization)
  - [Host & GIT variables](#host--git-variables)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Running the CI Jobs locally

The main goal of the CI philosophy we have that no matter how complex
the test and integration infrastructure, as a developer you should be
able to reproduce and re-run any of the failed checks locally. One part
of it are prek checks, that allow you to run the same static
checks in CI and locally, but another part is the CI environment which
is replicated locally with Breeze.

You can read more about Breeze in
[README.rst](../README.rst) but in essence it is a python wrapper around
docker commands that allows you (among others) to re-create CI environment
in your local development  instance and interact with it.
In its basic form, when you do development you can run all the same
tests that will be run in CI - but
locally, before you submit them as PR. Another use case where Breeze is
useful is when tests fail on CI.

All our CI jobs are executed via `breeze` commands. You can replicate
exactly what our CI is doing by running the sequence of corresponding
`breeze` command. Make sure however that you look at both:

- flags passed to `breeze` commands
- environment variables used when `breeze` command is run - this is
  useful when we want to set a common flag for all `breeze` commands in
  the same job or even the whole workflow. For example `VERBOSE`
  variable is set to `true` for all our workflows so that more detailed
  information about internal commands executed in CI is printed.

In the output of the CI jobs, you will find both - the flags passed and
environment variables set.

# Getting the CI image from failing job

Every contributor can also pull and run images being result of a specific
CI run in GitHub Actions. This is a powerful tool that allows to
reproduce CI failures locally, enter the images and fix them much
faster.

Note that this currently only works for AMD machines, not for ARM machines, but
this will change soon.

To load the image from specific PR, you can use the following command:

```bash
breeze ci-image load --from-pr 12345 --python 3.10 --github-token <your_github_token>
```

To load the image from specific run (for example 12538475388),
you can use the following command, find the run id from GitHub action runs.

```bash
breeze ci-image load --from-run 12538475388 --python 3.10 --github-token <your_github_token>
```

After you load the image, you can reproduce the very exact environment that was used in the CI run by
entering breeze container without mounting your local sources:

```bash
breeze shell --mount-sources skip [OPTIONS]
```

And you should be able to run any tests and commands interactively in the very exact environment that
was used in the failing CI run even without checking out sources of the failing PR.
This is a powerful tool to debug and fix CI issues.

You can also build the image locally by checking-out the branch of the PR that was used and running:

```bash
breeze ci-image build
```

You have to be aware that some of the PRs and canary builds use the `--upgrade-to-newer-dependencies` flag
(`UPGRADE_TO_NEWER_DEPENDENCIES` environment variable set to `true`) and they are not using constraints
to build the image so if you want to build it locally, you should pass the `--upgrade-to-newer-dependencies`
flag when you are building the image.

Note however, that if constraints changed for regular builds and if someone released a new package in PyPI
since the build was run (which is very likely - we have many packages released a day), the image you
build locally might be different than the one in CI, that's why loading image using `breeze ci-image load`
is more reliable way to reproduce the CI build.

If you check-out the branch of the PR that was used, regular ``breeze`` commands will
also reproduce the CI environment without having to rebuild the image - for example when dependencies
changed or when new dependencies were released and used in the CI job - and you will
be able to edit source files locally as usual and use your IDE and tools you usually use to develop Airflow.

In order to reproduce the exact job you also need to set the "[OPTIONS]" corresponding to the particular
job you want to reproduce within the run. You can find those in the logs of the CI job. Note that some
of the options can be passed by `--flags` and some via environment variables, for convenience, so you should
take a look at both if you want to be sure to reproduce the exact job configuration. See the next chapter
for summary of the most important environment variables and options used in the CI jobs.

You can read more about it in [Breeze](../README.rst) and [Testing](/contributing-docs/09_testing.rst)

# Options and environment variables used

Depending whether the scripts are run locally via [Breeze](../README.rst) or whether they are run in
`Build Images` or `Tests` workflows can behave differently.

You can use those variables when you try to reproduce the build locally - alternatively you can pass
those via corresponding command line flag passed to `breeze shell` command.

## Basic variables

Those variables are controlling basic configuration and behaviour of the breeze command.

| Variable                   | Option                   | Local dev | CI   | Comment                                                                      |
|----------------------------|--------------------------|-----------|------|------------------------------------------------------------------------------|
| PYTHON_MAJOR_MINOR_VERSION | --python                 |           |      | Major/Minor version of Python used.                                          |
| BACKEND                    | --backend                |           |      | Backend used in the tests.                                                   |
| INTEGRATION                | --integration            |           |      | Integration used in tests.                                                   |
| DB_RESET                   | --db-reset/--no-db-reset | false     | true | Determines whether database should be reset at the container entry.          |
| ANSWER                     | --answer                 |           | yes  | This variable determines if answer to questions should be automatically set. |

## Test variables

Those variables are used to control the test execution.

| Variable          | Option              | Local dev | CI                   | Comment                                   |
|-------------------|---------------------|-----------|----------------------|-------------------------------------------|
| RUN_DB_TESTS_ONLY | --run-db-tests-only |           | true in db tests     | Whether only db tests should be executed. |
| SKIP_DB_TESTS     | --skip-db-tests     |           | true in non-db tests | Whether db tests should be skipped.       |


## In-container environment initialization

Those variables are used to control the initialization of the environment in the container.

| Variable                        | Option                             | Local dev | CI        | Comment                                                                     |
|---------------------------------|------------------------------------|-----------|-----------|-----------------------------------------------------------------------------|
| MOUNT_SOURCES                   | --mount-sources                    |           | skip      | Whether to mount the local sources into the container.                      |
| SKIP_ENVIRONMENT_INITIALIZATION | --skip-environment-initialization  | false (*) | false (*) | Skip initialization of test environment (*) set to true in prek hooks.      |
| SKIP_IMAGE_UPGRADE_CHECK        | --skip-image-upgrade-check         | false (*) | false (*) | Skip checking if image should be upgraded (*) set to true in prek hooks.    |
| SKIP_PROVIDERS_TESTS            |                                    | false     | false     | Skip running provider integration tests (in non-main branch).               |
| SKIP_SSH_SETUP                  |                                    | false     | false (*) | Skip setting up SSH server for tests. (*) set to true in GitHub CodeSpaces. |
| VERBOSE_COMMANDS                |                                    | false     | false     | Whether every command executed in docker should be printed.                 |

## Host & GIT variables

Those variables are automatically set by Breeze when running the commands locally, but you can override them
if you want to run the commands in a different environment.

| Variable      | Local dev | CI         | Comment                                |
|---------------|-----------|------------|----------------------------------------|
| HOST_USER_ID  | Host UID  |            | User id of the host user.              |
| HOST_GROUP_ID | Host GID  |            | Group id of the host user.             |
| HOST_OS       | <from os> | linux      | OS of the Host (darwin/linux/windows). |
| COMMIT_SHA    |           | GITHUB_SHA | SHA of the commit of the build is run  |


----

**Thank you** for reading this far. We hope that you have learned a lot about Reproducing Airflow's CI job locally and CI in general.
