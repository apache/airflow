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
  - [GitHub Registry used as cache](#github-registry-used-as-cache)
  - [Authentication in GitHub Registry](#authentication-in-github-registry)
  - [GitHub Artifacts used to store built images](#github-artifacts-used-to-store-built-images)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CI Environment

Continuous Integration is an important component of making Apache Airflow
robust and stable. We run a lot of tests for every pull request,
for `canary` runs from `main` and `v*-\*-test` branches
regularly as scheduled jobs.

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
use the cache from GitHub Actions Artifacts.

## GitHub Registry used as cache

We are using GitHub Registry to store the last image built in canary run
to build images in CI and local docker container.
This is done to speed up the build process and to ensure that the
first - time-consuming-to-build layers of the image are
reused between the builds. The cache is stored in the GitHub Registry
by the `canary` runs and then used in the subsequent runs.

The latest GitHub registry cache is kept as `:cache-linux-amd64` and
`:cache-linux-arm64` tagged cache of our CI images (suitable for
`--cache-from` directive of buildx). It contains
metadata and cache for all segments in the image,
and cache is kept separately for different platforms.

The `latest` images of CI and PROD are `amd64` only images for CI,
because there is no easy way to push multiplatform images without
merging the manifests, and it is not really needed nor used for cache.

## Authentication in GitHub Registry

Authentication to GitHub Registry in CI uses GITHUB_TOKEN mechanism.
The Authentication is needed for  pushing the images (WRITE) in the `canary` runs.
When you are running the CI jobs in GitHub Actions, GITHUB_TOKEN is set automatically
by the actions. This is used only in the `canary` runs that have "write" access
to the repository.

No `write` access is needed (nor possible) by Pull Requests coming from the forks,
since we are only using "GitHub Artifacts" for cache source in those runs.

## GitHub Artifacts used to store built images

We are running most tests in reproducible CI image for all the jobs and
instead of build the image multiple times we build image for each python
version only once (one  CI and one PROD). Those images are then used by
All jobs that need them in the same build. The images - after building
are exported to a file and stored in the GitHub Artifacts.
The export files are then downloaded from artifacts and image is
loaded from the file in all jobs in the same workflow after they are
built and uploaded in the build image job.

----

Read next about [Images](02_images.md)
