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
[README.rst](../README.rst) but in essence it is a script
that allows you to re-create CI environment in your local development
instance and interact with it. In its basic form, when you do
development you can run all the same tests that will be run in CI - but
locally, before you submit them as PR. Another use case where Breeze is
useful is when tests fail on CI. You can take the full `COMMIT_SHA` of
the failed build pass it as `--image-tag` parameter of Breeze and it
will download the very same version of image that was used in CI and run
it locally. This way, you can very easily reproduce any failed test that
happens in CI - even if you do not check out the sources connected with
the run.

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

You can read more about it in [Breeze](../README.rst) and
[Testing](../../../../contributing-docs/09_testing.rst)

Since we store images from every CI run, you should be able easily
reproduce any of the CI tests problems locally. You can do it by pulling
and using the right image and running it with the right docker command,
For example knowing that the CI job was for commit
`cd27124534b46c9688a1d89e75fcd137ab5137e3`:

``` bash
docker pull ghcr.io/apache/airflow/main/ci/python3.9:cd27124534b46c9688a1d89e75fcd137ab5137e3

docker run -it ghcr.io/apache/airflow/main/ci/python3.9:cd27124534b46c9688a1d89e75fcd137ab5137e3
```

But you usually need to pass more variables and complex setup if you
want to connect to a database or enable some integrations. Therefore it
is easiest to use [Breeze](../README.rst) for that. For
example if you need to reproduce a MySQL environment in python 3.9
environment you can run:

``` bash
breeze --image-tag cd27124534b46c9688a1d89e75fcd137ab5137e3 --python 3.9 --backend mysql
```

You will be dropped into a shell with the exact version that was used
during the CI run and you will be able to run pytest tests manually,
easily reproducing the environment that was used in CI. Note that in
this case, you do not need to checkout the sources that were used for
that run - they are already part of the image - but remember that any
changes you make in those sources are lost when you leave the image as
the sources are not mapped from your host machine.

Depending whether the scripts are run locally via
[Breeze](../README.rst) or whether they are run in
`Build Images` or `Tests` workflows they can take different values.

You can use those variables when you try to reproduce the build locally
(alternatively you can pass those via corresponding command line flags
passed to `breeze shell` command.

| Variable                                | Local development  | Build Images workflow  | CI Workflow  | Comment                                                                        |
|-----------------------------------------|--------------------|------------------------|--------------|--------------------------------------------------------------------------------|
| Basic variables                         |                    |                        |              |                                                                                |
| PYTHON_MAJOR_MINOR_VERSION              |                    |                        |              | Major/Minor version of Python used.                                            |
| DB_RESET                                | false              | true                   | true         | Determines whether database should be reset at the container entry.            |
| Forcing answer                          |                    |                        |              |                                                                                |
| ANSWER                                  |                    | yes                    | yes          | This variable determines if answer to questions should be automatically given. |
| Host variables                          |                    |                        |              |                                                                                |
| HOST_USER_ID                            |                    |                        |              | User id of the host user.                                                      |
| HOST_GROUP_ID                           |                    |                        |              | Group id of the host user.                                                     |
| HOST_OS                                 |                    | linux                  | linux        | OS of the Host (darwin/linux/windows).                                         |
| Git variables                           |                    |                        |              |                                                                                |
| COMMIT_SHA                              |                    | GITHUB_SHA             | GITHUB_SHA   | SHA of the commit of the build is run                                          |
| In container environment initialization |                    |                        |              |                                                                                |
| SKIP_ENVIRONMENT_INITIALIZATION         | false*             | false*                 | false*       | Skip initialization of test environment * set to true in pre-commits           |
| SKIP_IMAGE_UPGRADE_CHECK                | false*             | false*                 | false*       | Skip checking if image should be upgraded * set to true in pre-commits         |
| SKIP_PROVIDER_TESTS                     | false*             | false*                 | false*       | Skip running provider integration tests                                        |
| SKIP_SSH_SETUP                          | false*             | false*                 | false*       | Skip setting up SSH server for tests. * set to true in GitHub CodeSpaces       |
| VERBOSE_COMMANDS                        | false              | false                  | false        | Determines whether every command executed in docker should be printed.         |
| Image build variables                   |                    |                        |              |                                                                                |
| UPGRADE_TO_NEWER_DEPENDENCIES           | false              | false                  | false*       | Determines whether the build should attempt to upgrade dependencies.           |

# Upgrade to newer dependencies

By default we are using a tested set of dependency constraints stored in separated "orphan" branches of the airflow repository
("constraints-main, "constraints-2-0") but when this flag is set to anything but false (for example random value),
they are not used used and "eager" upgrade strategy is used when installing dependencies. We set it to true in case of direct
pushes (merges) to main and scheduled builds so that the constraints are tested. In those builds, in case we determine
that the tests pass we automatically push latest set of "tested" constraints to the repository. Setting the value to random
value is best way to assure that constraints are upgraded even if there is no change to pyproject.toml
This way our constraints are automatically tested and updated whenever new versions of libraries are released.
(*) true in case of direct pushes and scheduled builds

----

**Thank you** for reading this far. We hope that you have learned a lot about Airflow's CI.
