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

- [Purpose of the document](#purpose-of-the-document)
- [Automated image cache and constraints refreshing in CI](#automated-image-cache-and-constraints-refreshing-in-ci)
- [Manually refreshing the image cache](#manually-refreshing-the-image-cache)
  - [Why we need to update image cache manually](#why-we-need-to-update-image-cache-manually)
  - [Prerequisites](#prerequisites)
  - [How to refresh the image cache](#how-to-refresh-the-image-cache)
  - [Is it safe to refresh the image cache?](#is-it-safe-to-refresh-the-image-cache)
  - [What the command does](#what-the-command-does)
- [Manually generating constraint files](#manually-generating-constraint-files)
  - [Why we need to generate constraint files manually](#why-we-need-to-generate-constraint-files-manually)
  - [How to generate constraint files](#how-to-generate-constraint-files)
  - [Is it safe to generate constraints manually?](#is-it-safe-to-generate-constraints-manually)
- [Manually updating already tagged constraint files](#manually-updating-already-tagged-constraint-files)
  - [Why we need to update constraint files manually (very rarely)](#why-we-need-to-update-constraint-files-manually-very-rarely)
  - [How to update the constraints](#how-to-update-the-constraints)
  - [Is it safe to update constraints manually?](#is-it-safe-to-update-constraints-manually)
  - [How the command works under-the-hood ?](#how-the-command-works-under-the-hood-)
  - [Examples of running the command](#examples-of-running-the-command)
- [Figuring out backtracking dependencies](#figuring-out-backtracking-dependencies)
  - [Why we need to figure out backtracking dependencies](#why-we-need-to-figure-out-backtracking-dependencies)
  - [How to figure out backtracking dependencies](#how-to-figure-out-backtracking-dependencies)
  - [Example backtracking session](#example-backtracking-session)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Purpose of the document

This document contains explanation of a few manual procedures we might use at certain times, to update
our CI and constraints manually when the automation of our CI is not enough. There are some edge cases
and events that might trigger the need of refreshing the information stored in our GitHub Repository.

We are storing two things in our GitHub Registry that are needed for both our contributors
and users:

* `CI and PROD image cache` - used by our CI jobs to speed up building of images while CI jobs are running
* `Constraints files` - used by both, CI jobs (to fix the versions of dependencies used by CI jobs in regular
  PRs) and used by our users to reproducibly install released airflow versions.

Normally, both are updated and refreshed automatically via [CI system](../dev/breeze/doc/ci/README.md).
However, there are some cases where we need to update them manually. This document describes how to do it.

# Automated image cache and constraints refreshing in CI

Our [CI](../dev/breeze/doc/ci/README.md) is build in the way that it self-maintains. Regular scheduled builds and
merges to `main` branch builds (also known as `canary` builds) have separate maintenance step that
take care about refreshing the cache that is used to speed up our builds and to speed up
rebuilding of [Breeze](./breeze/doc/README.rst) images for development purpose. This is all happening automatically, usually:

* The latest [constraints](../contributing-docs/12_airflow_dependencies_and_extras.rst#pinned-constraint-files) are pushed to appropriate branch after all tests succeed in the
  `canary` build.

* The [images](breeze/doc/ci/02_images.md) in `ghcr.io` registry are refreshed early at the beginning of the
  `canary` build. This is done twice during the canary build:
   * By the `Push Early Image Cache` job that is run at the beginning of the `canary` build. This cover the
     case when there are new dependencies added or Dockerfile/scripts change. Thanks to that step, subsequent
     PRs will be faster when they use the new Dockerfile/script. Those jobs **might fail** occasionally,
     if the latest PR added some conflicting dependencies with current constraints. This is not a problem
     and when it happens, it will be fixed by the next step.
   * By the `Push Image Cache` job that is run at the end of the `canary` build. This covers the case when
     cache is also refreshed after than `main` build succeeds after the new constraints are pushed. This
     step makes sure that constraints are committed and pushed just before the cache is refreshed, so
     there is no problem with conflicting dependencies.


# Manually refreshing the image cache

## Why we need to update image cache manually

Sometimes, when we have a problem with our CI running and flakiness of GitHub Actions runners or our
tests, the refresh might not be triggered. This has been mitigated by "Push Early Image Cache" job added in
our CI, but there are other reasons you might want to refresh the cache. Sometimes we want to refresh the
image cache in `vX_Y_test` branch (following our convention of branch names `vX_Y_test` branch is the branch
used to release all `X.Y.*` versions of airflow) before we attempt to push a change there.
There are no PRs happening in this branch, so manual refresh before we make a PR might speed up the PR build.
Or sometimes we just refreshed the constraints (see below) and we want the cache to include those.

## Prerequisites

Note that in order to refresh images you have to not only have `buildx` command installed for docker,
but you should also make sure that you have the buildkit builder configured and set.

If you just refresh image cache for your platform (AMD or ARM) - buildx is all you need, but if
you want to refresh the cache for both platforms, you need to have support for multi-platform builds
you need to have support for qemu or hardware ARM/AMD builders configured. The chapters below explain both options.

### Setting up cache refreshing with emulation

According to the [official installation instructions](https://docs.docker.com/buildx/working-with-buildx/#build-multi-platform-images)
this can be achieved via:

```bash
docker run --privileged --rm tonistiigi/binfmt --install all
```

More information can be found [here](https://docs.docker.com/engine/reference/commandline/buildx_create/)

However, emulation is very slow - more than 10x slower than hardware-backed builds.

### Setting up cache refreshing with hardware ARM/AMD support

If you plan to build  a number of images, probably better solution is to set up a hardware remote builder
for your ARM or AMD builds (depending which platform you build images on - the "other" platform should be
remote.

This  can be achieved by settings build as described in
[this guideline](https://www.docker.com/blog/speed-up-building-with-docker-buildx-and-graviton2-ec2/) and
adding it to docker buildx `airflow_cache` builder.

This usually can be done with those two commands:

```bash
docker buildx create --name airflow_cache   # your local builder
docker buildx create --name airflow_cache --append HOST:PORT  # your remote builder
```

One of the ways to have HOST:PORT is to login to the remote machine via SSH and forward the port to
the docker engine running on the remote machine.

When everything is fine you should see both local and remote builder configured and reporting status:

```bash
docker buildx ls

  airflow_cache          docker-container
       airflow_cache0    unix:///var/run/docker.sock
       airflow_cache1    tcp://127.0.0.1:2375
```

## How to refresh the image cache

The images can be rebuilt and refreshed after the constraints are pushed. Refreshing image for all
python version is as simple as running the [refresh_images.sh](refresh_images.sh) script which will
rebuild all the images in parallel and push them to the registry.

Note that you need to run `docker login ghcr.io` before you run the script and you need to be
a committer in order to be able to push the cache to the registry.

By default the command refreshes both ARM and AMD images:

```bash
./dev/refresh_images.sh
```

But you can also set PLATFORM variable if you want to refresh only single platform:

```bash
export PLATFORM=linux/amd64
./dev/refresh_images.sh
```

or

```bash
export PLATFORM=linux/arm64
./dev/refresh_images.sh
```


## Is it safe to refresh the image cache?

Yes. Image cache is only used to speed up the build process in CI. The worst thing that can happen if
the image cache is broken is that the PR builds of our will run slower - usually, for regular PRs building
the images from scratch takes about 15 minutes. With the image cache it takes about 1 minute if there are no
dependency changes. So if the image cache is broken, the worst thing that will happen is that the PR builds
will run longer "Wait for CI Image" step and "Wait for PROD image" will simply wait a bit longer.

Eventually the cache will heal itself. When the `main` build succeeds with all the tests, the cache is
automatically updated. Actually it's even faster in new CI process of ours, the cache is refreshed
very quickly after there is a merge of a new PR to the main ("Push Early Image Cache" jobs), so
cache refreshing and self-healing should be generally rather quick.

## What the command does

The command does the following:

* builds the CI image using the builders configured using buildx and pushes the cache
  to the `apache/airflow` registry (`--prepare-buildx-cache` flag). It builds all images in parallel for
  both AMD and ARM architectures.
* prepares packages and airflow packages in `dist` folder using the latest sources
* moves the packages to the `docker-context-files` folder so that they are available when building the
  PROD images
* builds the PROD image using the builders configured and packages prepared using buildx and pushes the cache
  to the `apache/airflow` registry (`--prepare-buildx-cache` flag). It builds all images in parallel for
  both AMD and ARM architectures.

# Manually generating constraint files

## Why we need to generate constraint files manually

Sometimes we want to generate constraint files if - for whatever reason - we cannot or do not want to wait
until `main` or `vY_Z_test` branch tests succeed. The constraints are only refreshed by CI when all the tests
pass, and this is a good thing, however there are some cases where we cannot solve some intermittent problem
with tests, but we KNOW that the tip of the branch is good and we want to release a new airflow version or
we want to move the PRs of contributors to start using the new constraints. This should be done with caution
and you need to be sure what you are doing, but you can always do it manually if you want.

## How to generate constraint files

```bash
breeze ci-image build --run-in-parallel --upgrade-to-newer-dependencies --answer yes
breeze release-management generate-constraints --airflow-constraints-mode constraints --run-in-parallel --answer yes
breeze release-management generate-constraints --airflow-constraints-mode constraints-source-providers --run-in-parallel --answer yes
breeze release-management generate-constraints --airflow-constraints-mode constraints-no-providers --run-in-parallel --answer yes

AIRFLOW_SOURCES=$(pwd)
```

The constraints will be generated in `files/constraints-PYTHON_VERSION/constraints-*.txt` files. You need to
check out the right 'constraints-' branch in a separate repository, and then you can copy, commit and push the
generated files.

You need to be a committer, and you have to be authenticated in the apache/airflow repository for your
git commands to be able to push the new constraints

```bash
cd <AIRFLOW_WITH_CONSTRAINTS-MAIN_DIRECTORY>
git pull
cp ${AIRFLOW_SOURCES}/files/constraints-*/constraints*.txt .
git diff
git add .
git commit -m "Your commit message here" --no-verify
git push
```

## Is it safe to generate constraints manually?

The slight risk is that if there is a constraint problem that impacts regular PRs and tests then it might
make all PRs "red" until the constraint is fixed. However, if this is the case then usually we should fix
the problem by fixing the tests or dependencies and the automated CI process should be able to self-heal.
The main build does not use constraints and it will attempt to upgrade (or downgrade) the dependencies to
the latest version matching the dependency specification we have in `pyproject.toml` files (note that provider
dependencies in `pyproject.toml` are generated from provider.yaml files being the single source of truth for
provider dependencies). Also, the constraints are pushed without `--force` so there is no risk of destroying
anything. The history is kept in Git, so you can always revert to the previous version if needed.

# Manually updating already tagged constraint files

## Why we need to update constraint files manually (very rarely)

Sometimes - very rarely - we need to fix historical constraint files when Airflow fails to install with the
constraints that were used in the past. This happened already several times and usually only happens when
there is a backwards-incompatible change in the build environment in Python installation toolchain
(pip, setuptools, wheel, Cython etc.). The Python build environment is not controllable by us - by default
pip uses `build isolation` which means that it will install the latest version of the build tools. Those
tools versions are chosen by `pip` separately for each package. However, this might mean that new versions of
such tools, released after the package has been released can break the installation. This happened for
example in July 2023 when major (3.0.0) version of Cython has been released and it
broke `pymssql` installation. We had to update the constraint files to use `pymssql==2.2.8` instead
of `pymssql==2.2.7` because version 2.2.7 did not limit but also did not work with the new version of Cython.
Version 2.2.8 of `pymssql` fixed compatibility with Cython 3.0.0, so replacing it in constraints brought back
the reproducibility of installation promised by constraints.

## How to update the constraints

Breeze has `update-constraints` command in `release-management` group that can be used to update the
constraints in bulk.

This is a step-by-step instruction on how to use it:

1. You need to have "airflow" repository checked out separately from the repository you are working on. For
   example in `/home/myuser/airflow-constraints` folder.
2. You need to checkout `constraints-main` branch in this repository. By default the command expects that
   there is a remote named "apache" pointing to the official Apache repository. You can override this
    by passing `--remote-name` option to the command.
3. You need to run `breeze release-management update-constraints` command. The `breeze` command comes usually
   from another clone of airflow repository - usually from the `main` branch. You should pass those options to
   the command:
      * path to the "constraints" repository
      * remote name where the constraints should be pushed (optionally - default "apache")
      * list of airflow versions to update constraints for
      * list of constraints to update in the form of "package==version" (you can specify it multiple times)
      * message to be used in the commit message

   Make sure you use exactly the same form of the name for the package to be updated as the one already in the
   constraints. PyPI normalizes names of packages and sometimes you can see different variants of it - for
   example `pyaml` vs. `PyYaml`. Check what is currently stored in constraints for the package you want to
   update and use exactly the same form of the package name.

4. Verify manually if the change is visible as expected by inspecting the constraints at:

https://github.com/apache/airflow/tree/constraints-<airflow-version>

## Is it safe to update constraints manually?

The command is designed with safety in mind. Even if you make a mistake there is always a way back. But
there are a few ways you can check that what you are doing is going to work as expected. Read on.

It's a good idea to add `--dry-run` option to the command to see what will be updated before you actually
run the command.

However, even if you do not use `--dry-run` option, the command will ask you to
confirm the updates so you will have a chance to verify it before each version change.

When you run the command for the first time you can also add `--verbose` instead of `--dry-run` and
you will see the git commands being executed by the command while it is doing its job.

Airflow constraint tags are moved with ``--force`` option - this needs to be done because we are moving already
existing tag, however branches are pushed without force so there is no risk of losing history in the repository.
You can always see the history and revert the changes and restore old tags manually. Usually the "final" tags
are the same as the latest "rc*" tags for the same version so it is easy to find where the tag was
pointing before - we also print hash of the commits before attempting to make modifications so you can
always see what commit the tag has been pointing to before the command is run.

## How the command works under-the-hood ?

The command will do the following for every Airflow version specified:

  * checkout "constraints-<version>" tag
  * reset "constraints-<version>-fix" branch to the tag
  * update constraints in-place
  * commit the changes
  * tag the commit with "constraints-<version>" tag
  * push the "constraints-<version>-fix" branch with the commit to the remote selected
  * push the tag to the remote selected


## Examples of running the command

Example of updating constraints for Airflow 2.5.0 - 2.6.3 and updating `pymssql` constraint to 2.2.8:

```bash
breeze release-management update-constraints --constraints-repo /home/user/airflow-constraints \
    --airflow-versions 2.5.0,2.5.1,2.5.2,2.5.3,2.6.0,2.6.1,2.6.2,2.6.3 \
    --updated-constraint pymssql==2.2.8 \
    --commit-message "Update pymssql constraint to 2.2.8" \
    --airflow-constraints-mode constraints
```

Example of updating multiple constraints:

```bash
breeze release-management update-constraints --constraints-repo /home/user/airflow-constraints \
    --airflow-versions 2.5.0,2.5.1,2.5.2,2.5.3,2.6.0,2.6.1,2.6.2,2.6.3 \
    --updated-constraint pymssql==2.2.8 \
    --updated-constraint Authlib==1.3.0 \
    --commit-message "Update pymssql constraint to 2.2.8 and Authlib to 1.3.0" \
    --airflow-constraints-mode constraints
```


# Figuring out backtracking dependencies

## Why we need to figure out backtracking dependencies

Very rarely the CI image in `canary` builds take a very long time to build. This is usually
caused by `pip` trying to figure out the latest set of dependencies (`eager upgrade`) .
The resolution of dependencies is a very complex problem and sometimes it takes a long time to figure out
the best set of dependencies. This is especially true when we have a lot of dependencies and they all have
to be found compatible with each other. In case new dependencies are released, `pip` might enter
a long loop trying to figure out if these dependencies can be used. Unfortunately, this long loop could end up in an error due to conflicts.

This is visible in the "build output" as `pip` attempting to continuously backtrack and download many new
versions of various dependencies, trying to find a good match.

This is why we need to help pip to skip newer versions of those dependencies, until the
condition that caused the backtracking is solved.

We do it by adding `dependency<=version` to the EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS variable in
`Dockerfile.ci`. The trick is to find the dependency that is causing the backtracking.

We use a "bisecting" methodology to test candidates for backtrack triggering among those released in PyPI since the last successful run
of ``--upgrade-to-newer-dependencies`` and committed the constraints in the `canary` build.

## How to figure out backtracking dependencies

First - we have a breeze command that can help us with that:

```bash
breeze ci find-backtracking-candidates
```

This command should be run quickly after noticing that the CI build is taking a long time and failing. It relies on the fact that an eager upgrade produced valid constraints at some point of time, so it tries to identify which dependencies have been added since then. By doing it, it limits them to the versions used in the constraints.

Instead of running the command manually, you could also rely on the failing CI builds. We run the
`find-backtracking-candidates` command in the `canary` build when it times out, so the
easiest way to find backtracking candidates is to find the first build that failed with timeout. It
will likely have the smallest number of backtracking candidates. The command outputs the limitation
for those backtracking candidates that are guaranteed to work (because they are taken from the latest
constraints and they already succeeded in the past when the constraints were updated).

Then, we run ``breeze ci-image build --upgrade-to-newer-dependencies --eager-upgrade-additional-requirements "REQUIREMENTS"``
to check which of the candidates causes the long builds. Initially, you put there the whole list of
candidates that you got from the `find-backtracking-candidates` command. This **should** succeed. Now,
the next step is to narrow down the list of candidates to the one that is causing the backtracking.

We narrow-down the list by "bisecting" the list. We remove half of the dependency limits and see if it
still works or not. If it works, we continue. Otherwise, we restore the removed half and remove
the other half. Repeat until there is only one dependency left - hopefully
(sometimes you will need to leave few of them).

This way we can relatively quickly narrow down the dependency that is causing the backtracking. Once we
figure out which dependency is causing it, we can attempt to figure it out why it is causing the backtracking
by specifying the latest released version of the dependency as `== <latest released version>` in the
`--eager-upgrade-additional-requirements`. This should rather quickly fail and `pip` should show us what
the dependency is conflicting with. There might be multiple reasons for that. Most often it is simply
a dependency that has a requirement that is limited and we need to wait until new version of that
dependency is released.

Note that - such build **might** even succeed - surprisingly. Then this is simply a sign that `pip`
algorithm for `--eager-upgrade` was not perfect and the solution could be found given sufficient time.
In such case it might also be that removing the limit in the next few days will not cause the backtracking.

Finally, in order to make the change permanent in our CI builds, we should add the limitation to the
`EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS` arg in `Dockerfile.ci` and commit the change. We usually commit
the limits with `<VERSION` suffix (where version is the version that causes backtracking - usually that will
be the latest released version, unless that dependency had quick subsequent releases - you can try it before
committing by simply adding it to `EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS` in Dockerfile.ci and
running `breeze ci-image build --upgrade-to-newer-dependencies`. Make sure to add the comment explaining
when we should remove the limit.


Later on - periodically we might attempt to remove the limitation and see if the backtracking is still
happening. If it is not - we just remove the limitation from `Dockerfile.ci` and commit the change.

## Example backtracking session

This is the example backtracking session run on 13th of August 2023 after the `canary` CI image build
started to fail with timeout a day before.

1. The `breeze ci-image build --upgrade-to-newer-dependencies` failed on CI after 80 minutes.

2. The output of the `breeze ci find-backtracking-candidates` command:

```
Last constraint date: 2023-08-09 21:48:23
Latest version aiobotocore==2.6.0 release date: 2023-08-11 20:43:19. In current constraints: 2.5.4)
Latest version asana==4.0.5 release date: 2023-08-11 18:56:04. In current constraints: 3.2.1)
Latest version async-timeout==4.0.3 release date: 2023-08-10 16:35:55. In current constraints: 4.0.2)
Latest version aws-sam-translator==1.73.0 release date: 2023-08-10 00:01:00. In current constraints: 1.72.0)
Latest version azure-core==1.29.1 release date: 2023-08-10 05:09:59. In current constraints: 1.29.0)
Latest version azure-cosmos==4.5.0 release date: 2023-08-09 23:43:07. In current constraints: 4.4.0)
Latest version boto3==1.28.25 release date: 2023-08-11 19:23:52. In current constraints: 1.28.17)
Latest version botocore==1.31.25 release date: 2023-08-11 19:23:34. In current constraints: 1.31.17)
Latest version cfgv==3.4.0 release date: 2023-08-12 20:38:16. In current constraints: 3.3.1)
Latest version coverage==7.3.0 release date: 2023-08-12 18:34:06. In current constraints: 7.2.7)
Latest version databricks-sql-connector==2.9.1 release date: 2023-08-11 17:32:12. In current constraints: 2.8.0)
Latest version google-ads==21.3.0 release date: 2023-08-10 18:10:22. In current constraints: 21.2.0)
Latest version google-cloud-aiplatform==1.30.1 release date: 2023-08-11 21:19:50. In current constraints: 1.29.0)
Latest version grpcio-status==1.57.0 release date: 2023-08-10 15:54:17. In current constraints: 1.56.2)
Latest version grpcio==1.57.0 release date: 2023-08-10 15:51:52. In current constraints: 1.56.2)
Latest version mypy==1.5.0 release date: 2023-08-10 12:46:43. In current constraints: 1.2.0)
Latest version pyzmq==25.1.1 release date: 2023-08-10 09:01:18. In current constraints: 25.1.0)
Latest version tornado==6.3.3 release date: 2023-08-11 15:21:47. In current constraints: 6.3.2)
Latest version tqdm==4.66.1 release date: 2023-08-10 11:38:57. In current constraints: 4.66.0)
Latest version virtualenv==20.24.3 release date: 2023-08-11 15:52:32. In current constraints: 20.24.1)

Found 20 candidates for backtracking

Run `breeze ci-image --upgrade-to-newer-dependencies --eager-upgrade-additional-requirements "aiobotocore<=2.5.4 asana<=3.2.1 async-timeout<=4.0.2 aws-sam-translator<=1.72.0 azure-core<=1.29.0
azure-cosmos<=4.4.0 boto3<=1.28.17 botocore<=1.31.17 cfgv<=3.3.1 coverage<=7.2.7 databricks-sql-connector<=2.8.0 google-ads<=21.2.0 google-cloud-aiplatform<=1.29.0 grpcio-status<=1.56.2 grpcio<=1.56.2
mypy<=1.2.0 pyzmq<=25.1.0 tornado<=6.3.2 tqdm<=4.66.0 virtualenv<=20.24.1"`. It should succeed.
```

3. As instructed, run:

```bash
breeze ci-image build --upgrade-to-newer-dependencies --eager-upgrade-additional-requirements "\
aiobotocore<=2.5.4 asana<=3.2.1 async-timeout<=4.0.2 aws-sam-translator<=1.72.0 \
azure-core<=1.29.0 azure-cosmos<=4.4.0 boto3<=1.28.17 botocore<=1.31.17 cfgv<=3.3.1 coverage<=7.2.7 \
databricks-sql-connector<=2.8.0 google-ads<=21.2.0 google-cloud-aiplatform<=1.29.0 \
grpcio-status<=1.56.2 grpcio<=1.56.2 mypy<=1.2.0 pyzmq<=25.1.0 tornado<=6.3.2 tqdm<=4.66.0 virtualenv<=20.24.1"
```

The build succeeded in ~ 8 minutes.

4. Removed the second half:

```
breeze ci-image build --upgrade-to-newer-dependencies --eager-upgrade-additional-requirements "\
aiobotocore<=2.5.4 asana<=3.2.1 async-timeout<=4.0.2 aws-sam-translator<=1.72.0 \
azure-core<=1.29.0 azure-cosmos<=4.4.0 boto3<=1.28.17 botocore<=1.31.17 cfgv<=3.3.1 coverage<=7.2.7"
```

The build succeeded in ~ 8 minutes.

5. Removed the second half:

```
breeze ci-image build --upgrade-to-newer-dependencies --eager-upgrade-additional-requirements "\
aiobotocore<=2.5.4 asana<=3.2.1 async-timeout<=4.0.2 aws-sam-translator<=1.72.0"
```

The build succeeded in ~ 8 minutes.

6. Removed the second half:

```
breeze ci-image build --upgrade-to-newer-dependencies \
--eager-upgrade-additional-requirements "aiobotocore<=2.5.4 asana<=3.2.1"
```

The build succeeded in ~ 8 minutes.

6. Removed aiobotocore

```
asana<=3.2.1
```

The image build continued running way past 10 minutes and downloading many versions  of many dependencies.

7. Removed asana and restored aiobotocore

```
aiobotocore<=2.5.4
```

The build succeeded. Aiobotocore is our culprit.

8. Check the reason for backtracking (using latest released version of aiobotocore):

```bash
breeze ci-image build --upgrade-to-newer-dependencies --eager-upgrade-additional-requirements "aiobotocore==2.6.0"
```

Note. In this case the build succeeded, which means that this was simply a flaw in the `pip` resolution
algorithm (which is based on some heuristics) and not a real problem with the dependencies. We will
attempt to remove the limit in the next few days to see if the problem is resolved by other dependencies
released in the meantime.

9. Updated additional dependencies in `Dockerfile.ci` with appropriate comment:

```
# aiobotocore is limited temporarily until it stops backtracking pip
ARG EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS="aiobotocore<2.6.0"
```
