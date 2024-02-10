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

- [CI Environment](#ci-environment)
  - [GitHub Actions workflows](#github-actions-workflows)
  - [Container Registry used as cache](#container-registry-used-as-cache)
  - [Authentication in GitHub Registry](#authentication-in-github-registry)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CI Environment

Continuous Integration is an important component of making Apache Airflow
robust and stable. We run a lot of tests for every pull request,
for main and v2-\*-test branches and regularly as scheduled jobs.

Our execution environment for CI is [GitHub Actions](https://github.com/features/actions). GitHub Actions.

However. part of the philosophy we have is that we are not tightly
coupled with any of the CI environments we use. Most of our CI jobs are
written as Python code packaged in [Breeze](../../README.md) package,
which are executed as steps in the CI jobs via `breeze` CLI commands.
And we have a number of  variables determine build behaviour.

## GitHub Actions workflows

Our CI builds are highly optimized, leveraging the latest features
provided by the GitHub Actions environment to reuse parts of the build
process across different jobs.

A significant portion of our CI runs utilize container images. Given
that Airflow has numerous dependencies, we use Docker containers to
ensure tests run in a well-configured and consistent environment. This
approach is used for most tests, documentation building, and some
advanced static checks. The environment comprises two types of images:
CI images and PROD images. CI images are used for most tests and checks,
while PROD images are used for Kubernetes tests.

To run the tests, we need to ensure that the images are built using the
latest sources and that the build process is efficient. A full rebuild
of such an image from scratch might take approximately 15 minutes.
Therefore, we've implemented optimization techniques that efficiently
use the cache from the GitHub Docker registry. In most cases, this
reduces the time needed to rebuild the image to about 4 minutes.
However, when dependencies change, it can take around 6-7 minutes, and
if the base image of Python releases a new patch-level, it can take
approximately 12 minutes.

## Container Registry used as cache

We are using GitHub Container Registry to store the results of the
`Build Images` workflow which is used in the `Tests` workflow.

Currently in main version of Airflow we run tests in all versions of
Python supported, which means that we have to build multiple images (one
CI and one PROD for each Python version). Yet we run many jobs (\>15) -
for each of the CI images. That is a lot of time to just build the
environment to run. Therefore we are utilising the `pull_request_target`
feature of GitHub Actions.

This feature allows us to run a separate, independent workflow, when the
main workflow is run -this separate workflow is different than the main
one, because by default it runs using `main` version of the sources but
also - and most of all - that it has WRITE access to the GitHub
Container Image registry.

This is especially important in our case where Pull Requests to Airflow
might come from any repository, and it would be a huge security issue if
anyone from outside could utilise the WRITE access to the Container
Image Registry via external Pull Request.

Thanks to the WRITE access and fact that the `pull_request_target` workflow named
`Build Imaages` which - by default - uses the `main` version of the sources.
There we can safely run some code there as it has been reviewed and merged.
The workflow checks-out the incoming Pull Request, builds
the container image from the sources from the incoming PR (which happens in an
isolated Docker build step for security) and pushes such image to the
GitHub Docker Registry - so that this image can be built only once and
used by all the jobs running tests. The image is tagged with unique
`COMMIT_SHA` of the incoming Pull Request and the tests run in the `pull` workflow
can simply pull such image rather than build it from the scratch.
Pulling such image takes ~ 1 minute, thanks to that we are saving a
lot of precious time for jobs.

We use [GitHub Container Registry](https://docs.github.com/en/packages/guides/about-github-container-registry).
A `GITHUB_TOKEN` is needed to push to the registry. We configured
scopes of the tokens in our jobs to be able to write to the registry,
but only for the jobs that need it.

The latest cache is kept as `:cache-linux-amd64` and `:cache-linux-arm64`
tagged cache of our CI images (suitable for `--cache-from` directive of
buildx). It contains  metadata and cache for all segments in the image,
and cache is kept separately for different platform.

The `latest` images of CI and PROD are `amd64` only images for CI,
because there is no easy way to push multiplatform images without
merging the manifests, and it is not really needed nor used for cache.

## Authentication in GitHub Registry

We are using GitHub Container Registry as cache for our images.
Authentication uses GITHUB_TOKEN mechanism. Authentication is needed for
pushing the images (WRITE) only in `push`, `pull_request_target`
workflows. When you are running the CI jobs in GitHub Actions,
GITHUB_TOKEN is set automatically by the actions.

----

Read next about [Images](02_images.md)
