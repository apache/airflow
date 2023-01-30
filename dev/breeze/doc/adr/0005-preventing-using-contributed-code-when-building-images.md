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

- [5. Preventing using contributed code when building images](#5-preventing-using-contributed-code-when-building-images)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 5. Preventing using contributed code when building images

Date: 2021-12-19

## Status

Accepted

Builds on [4. Using Docker images as test environment](0004-using-docker-images-as-test-environment.md)

## Context

As described in [4. Using Docker images as test environment](0004-using-docker-images-as-test-environment.md),
Airflow CI system uses CI Docker image as consistent test execution environment. This environment provides
cacheability and rebuild capabilities that allow the image to be rebuilt quickly, incrementally based on
previous version of the images - whenever any of the source code, Python dependencies, System dependencies
are changed (in optimal way depending on the change). However, even with optimalizations, rebuilding
the image might take quite some time (when only sources change ~ 1 minute, but when system dependencies
change ~ 10 minutes). In certain cases we run (for the same Python version) 20 jobs that require the same
image as the environment, which in extreme cases would mean 20x10 = 200 build minutes on CI to
just rebuild the same image.

Therefore, there is a need to use the process that will allow to build the images once and share it with
all the test jobs that need it. Another advantage of having such image is that since the image is stored
in the registry with "commit" tag which allows to easily reproduce the environment used in particular
build on CI. This is a nice side effect of such setup, one that can be useful in case a user does not want
to lose time on checking out and rebuilding the environment locally for a build that comes from their own,
or another developer's PR.

This requires those prerequisites:

  * the images need to be built in a workflow that has "write" access to store the images after they are
    built, so that the images can then be pulled by the test jobs rather than rebuilt

  * the process to build the images need to be secured from malicious users that would like to inject a
    code in the build process to make bad use of the "write" access - for example to push the code
    to the repository or to inject malicious code to "common" artifacts used by the jobs

  * however, in order to build the images that reflect the PR of the user, they should be able to modify some
    code that is usually used to build airflow packages (`setup.py`, airflow sources, scripts).

GitHub Actions provide some features that we can use for that purpose:

* there is a "pull request target" workflow that uses only the code present in the protected "main" version
  of the code even if the code is modified in the PR. That code also has access to secrets stored in
  Airflow repository (for example in our case secret used to push documentation to S3 Bucket). Those
  secrets should not be made available to user code coming from PR.

* this "pull request target" workflow can have granular "write" permissions assigned, so that each job
  can be granted access to certain resources only

* however, they are ways (using some GitHub Actions) to inject more permissions - for example
  when "checkout" is performed by default using GitHub Actions checkout command by default the checked-out
  repository has "write" access and any of the further steps in the job can "push" using this repository
  (see https://cwiki.apache.org/confluence/display/BUILDS/GitHub+Actions+status#GitHubActionsstatus-Security)
  for details.

* when using 3rd-party actions, you need to "pin" the actions to specific COMMIT SHA versions because
  there is a risk, a 3rd-party might inject a code to your workflow by releasing and tagging new version
  of their actions: https://docs.github.com/en/actions/security-guides/security-hardening-for-github-actions

* some code that should be executed "inside" of the images when building should be possible to come from the
  PR - for example code that is used inside the docker image to install mysql or postgres should be possible
  to be changed in the PR - as long as we make sure the code is executed inside the Docker image or Docker
  build process.

Those protections are gradually strengthened (up until recently there was no granular access rights possible)
however we decided to add certain rules of the "build" code that is executed in our GitHub "Build Images"
"pull request target" to make sure

## Decision

The decision of our use of GitHub images is to utilise "Pull Request Workflow" to build the shared image,
but to make sure that the following rules are in-place:

1) We always use `persist-credentials: false` in all GitHub Action checkouts, to prevent unauthorized pushes
   to our repository

```yaml
      - uses: actions/checkout@v3
        with:
          ref: ${{ env.TARGET_COMMIT_SHA }}
          persist-credentials: false
          fetch-depth: 2
```

2) we use submodules (in .github/actions) where we keep actions that we are using (except of the standard
   GitHub managed actions). Submodules provide few features such as - automated linking to specific commit
   SHA (not tag) and integration with Pull Request Review process when someone creates a PR to upgrade the
   action, which makes it ideal to securely and seamlessly keep the action updated if needed.

3) No user code coming from the PR can be executed directly in the "Build image" workflow. For example, the
   build scripts should not import `setup.py` or execute bash scripts coming from other places than:
   * scripts/ci
   * dev/
   All the other sources are taken from the PR, but those two folders, during the "Build image" workflow
   are overwritten by the `main` version of the scripts. This means that it is safe to execute those
   scripts in the "Build Image" workflow.

4) All the other code coming from the PR can only be executed inside the docker container started by the
   `scripts/ci` or `dev` scripts. The docker containers should not have any volumes mounted from
   the host that will enable them to read or modify values, environment variables that are present in the
   Host CI environment.

5) The "docker build" commands automatically execute Dockerfile commands inside such a container, so there
   is no risk that sensitive information from the host will be passed to them.

6) In case any information is needed from the "sources" of Airflow (such as name of the current branch or
   version of airflow) it should be extracted by parsing of the incoming scripts but not executing them. Under
   any circumstances any of the scripts coming from the outside of `scripts/ci` and `dev` should be
   executed on the host during the image building process.

Unfortunately, this setup means that any changes in `scripts/ci` and `dev` that are needed in
the `Build Workflow` cannot be effective until they are merged. So if you have a change in one of those
that should affect the build workflow, it has to be tested by pushing the `main` branch into your own
repository fork in order to make it "eligible for running".

## Consequences

As a consequence of that decision, we should have optimized build of images which can be reused during
multiple CI jobs of the same PR build. This should decrease the waiting time for the result of the CI
tests as well as decrease the amount of build time needed to run the CI tests.

Thanks to combination of features available in GitHub, the builds are secured against potentially injected
code by users contributing PRs, that could get uncontrolled write access to Airflow repository.

The negative consequence of this is that the build process becomes much more complex
(see [CI](../../../../CI.rst) for complete description) and that some cases (like modifying build behaviour
require additional process of testing by pushing the changes as `main` branch to a fork of Apache Airflow)
