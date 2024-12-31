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
  - [Basic variables](#basic-variables)
  - [Host & GIT variables](#host--git-variables)
  - [In-container environment initialization](#in-container-environment-initialization)
- [Image build variables](#image-build-variables)
- [Upgrade to newer dependencies](#upgrade-to-newer-dependencies)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Running the CI Jobs locally

The main goal of the CI philosophy we have that no matter how complex
the test and integration infrastructure, as a developer you should be
able to reproduce and re-run any of the failed checks locally. One part
of it are pre-commit checks, that allow you to run the same static
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

Every contributor can also pull and run images being result of a specific
CI run in GitHub Actions. This is a powerful tool that allows to
reproduce CI failures locally, enter the images and fix them much
faster. It is enough to download and uncompress the artifact that stores the
image and run ``breeze ci-image load -i <path-to-image.tar> --python python``
to load the  image and mark the image as refreshed in the local cache.

You can read more about it in [Breeze](../README.rst) and
[Testing](../../../../contributing-docs/09_testing.rst)

Depending whether the scripts are run locally via
[Breeze](../README.rst) or whether they are run in
`Build Images` or `Tests` workflows they can take different values.

You can use those variables when you try to reproduce the build locally
(alternatively you can pass those via corresponding command line flags
passed to `breeze shell` command.

## Basic variables

| Variable                    | Local dev | CI   | Comment                                                                      |
|-----------------------------|-----------|------|------------------------------------------------------------------------------|
| PYTHON_MAJOR_MINOR_VERSION  |           |      | Major/Minor version of Python used.                                          |
| DB_RESET                    | false     | true | Determines whether database should be reset at the container entry.          |
| ANSWER                      |           | yes  | This variable determines if answer to questions should be automatically set. |

## Host & GIT variables

| Variable          | Local dev | CI         | Comment                                 |
|-------------------|-----------|------------|-----------------------------------------|
| HOST_USER_ID      | Host UID  |            | User id of the host user.               |
| HOST_GROUP_ID     | Host GID  |            | Group id of the host user.              |
| HOST_OS           | <from os> | linux      | OS of the Host (darwin/linux/windows).  |
| COMMIT_SHA        |           | GITHUB_SHA | SHA of the commit of the build is run   |

## In-container environment initialization

| Variable                        | Local dev | CI        | Comment                                                                     |
|---------------------------------|-----------|-----------|-----------------------------------------------------------------------------|
| SKIP_ENVIRONMENT_INITIALIZATION | false (*) | false (*) | Skip initialization of test environment (*) set to true in pre-commits.     |
| SKIP_IMAGE_UPGRADE_CHECK        | false (*) | false (*) | Skip checking if image should be upgraded (*) set to true in pre-commits.   |
| SKIP_PROVIDERS_TESTS            | false     | false     | Skip running provider integration tests.                                    |
| SKIP_SSH_SETUP                  | false     | false (*) | Skip setting up SSH server for tests. (*) set to true in GitHub CodeSpaces. |
| VERBOSE_COMMANDS                | false     | false     | Whether every command executed in docker should be printed.                 |

# Image build variables

| Variable                        | Local dev | CI        | Comment                                                            |
|---------------------------------|-----------|-----------|--------------------------------------------------------------------|
| UPGRADE_TO_NEWER_DEPENDENCIES   | false     | false (*) | Whether dependencies should be upgraded. (*) set in CI when needed |

# Upgrade to newer dependencies

By default, we are using a tested set of dependency constraints stored in separated "orphan" branches of the airflow repository
("constraints-main, "constraints-2-0") but when this flag is set to anything but false (for example random value),
they are not used and "eager" upgrade strategy is used when installing dependencies. We set it to true in case of direct
pushes (merges) to main and scheduled builds so that the constraints are tested. In those builds, in case we determine
that the tests pass we automatically push latest set of "tested" constraints to the repository. Setting the value to random
value is best way to assure that constraints are upgraded even if there is no change to pyproject.toml
This way our constraints are automatically tested and updated whenever new versions of libraries are released.
(*) true in case of direct pushes and scheduled builds

----

**Thank you** for reading this far. We hope that you have learned a lot about Airflow's CI.
