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

- [4. Using Docker images as test environment](#4-using-docker-images-as-test-environment)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 4. Using Docker images as test environment

Date: 2021-12-19

## Status

Accepted

Also see [5. Preventing using contributed code when building images](0005-preventing-using-contributed-code-when-building-images.md)

## Context

Airflow is not only an application but also a complex "ecosystem" that integrates with multiple external
services. It is split into multiple packages (main `apache-airflow` package and more than 70 provider
packages). Each of those packages brings its own set of dependencies and when you count all the transitive
dependencies they bring, it totals to more than 550 dependencies.

This number of dependencies is huge, and it makes it very difficult to keep the dependencies in
sync for all the developers that are contributing to Airflow. Airflow developers are coming with
different background and experiences. They also use various development environments - Linux,
Intel-based MacOS, WSL2 on Windows, soon also Windows, soon ARM-based MacOS. A lot of the dependencies
of Airflow are data-science related and optimized using "C" libraries (numpy, pandas and others) which
makes it extremely painful to set up and keep the virtual environments updated when you want to run
tests for all the components of Airflow. Another dependencies of Airflow (mysql, postgres and others)
also require "system-level" dependencies to be installed, not only Python dependencies (for example
mysql integration requires mysql client system libraries to be installed). Those dependencies that have
system dependencies change over time (for example when we added MsSQL support) so the environment needs
to include also evolution of those.

The usual, simple approach where each developer keeps their own virtualenv updated in this case does not
scale for Airflow. There are literally more than 10 dependencies updated every day for airflow and keeping
up with all those dependencies is next to impossible - for both casual users and seasoned committers.
Relying on the set of dependencies each developer in the past led often to "works for me" syndrome - where
tests executed worked fine for one developer but failed on CI and for other developers and only deep
investigation could reveal that single - seemingly unrelated - dependency had different version for one
of the developers.

Also, some tools that we use (for example mypy) require all dependencies to be installed and present
in precisely the same versions in order to produce repeatable results because they rely
on information present in the libraries (mypy relies on type hinting present in the libraries).
That means that if - for example - we want to have consistent `mypy` errors seen by different developers,
they should have the same versions of all dependencies and libraries that are used in CI. In the past it
has been a major source of confusion and problems when the errors reported in CI were not locally
reproducible (because again - seemingly unrelated dependency was installed in a different version)

Observations of this problem led to conclusion that we need a common development environment that will be
the common source of truth for the "current" set of dependencies in "main". This development environment
should have the following characteristics:

* It should be fully cross-platform, including an easy way to install and update dependencies without
  complex additional prerequisites and "dealing" with platform-specific issues.
* It should support multiple Python versions out of the box
* It should support running all the tests we have in Airflow - unit, integration, system tests
* It should be automatically used by the CI system of ours so that errors in CI can be reproduced locally
* Each developer should be able to easily synchronize the environment with the one that is used in CI
* Each developer should be able to contribute to the environment by adding new/changing dependencies
* Other developers should be able to easily get the updated environment after changes from other developers
  are merged.
* The environment should be cacheable for the CI speed purpose. Installing 500 dependencies takes a lot of
  time - and there should be an easy way to incrementally add new dependencies to the environment
  as PRs are adding dependencies. It should take minimum time to update the environment with minimum time
  needed for different cases:
  * when only sources change, rebuild should not be needed at all
  * when Python dependencies change, rebuilds should be incremental only without rebuilding all dependencies
  * only when node modules changes, the node modules and javascript `transpilation` (which takes quite
    some time) should be performed.
  * some dependencies only when some new, system requirements change, the environment might be rebuilt
    from scratch.
* The environment should also provide a way to test Airflow with the latest security fixes installed. Airflow,
  due to its hundreds of dependencies is very prone to "supply chain" attacks, therefore it is crucial that
  all the dependencies that are possible to upgrade, are upgraded as soon as possible, and they are used in
  CI and local development to make sure, that Airflow continues to work with the newest versions of the
  dependencies possibly containing security fixes.

## Decision

We decided to use Docker Images to provide such a development environment. Docker images are perfectly
suited for the job:

* they are fully cross-platform (including support for multiple architectures)
* they are easily shareable (docker push/pull)
* we have a registry (`ghcr.io`) in GitHub where we can keep the images for both CI and development
* they are cacheable, providing the right layering of the images
* they provide a mechanism to not only synchronize Python dependencies but also System dependencies
* they provide an easy mechanism to query and inspect the images
* the concept of the CI environment might also be extended to provide similar Production Image (but with
  slightly different characteristics
* they can be easily built and used in CI including caching support
* management and automated synchronisation of the environment can be easily done using simple, external
  scripts that will check the "freshness" of the images and propose corrective actions (pulling or
  rebuilding the image) for the user whenever they are needed (all that in cross-platform way)
* we can use specific base image (Debian bookworm with Long Term Support) and rely on security fixes provided
  by Debian for system dependencies.

The only alternative considered is a plain virtualenv managed individually by each user. The problem with
that in case of Airflow is a number of dependencies to manage and extremely weak cross-platform support
that virtualenv provide in case of many Airflow dependencies. The Virtualenv environment is good for many
local development needs, however in order to reliably recreate the same development environment that is
on CI and in order to run all the tests, virtualenv in conjunction with many C-level and system
dependencies is to "brittle" to make it "stable" and "repeatable" environment.

## Consequences

As a consequence of choosing Docker image as common development environment, the Airflow development
ecosystem gets fast, robust and seamless way to make sure that all the developers of Airflow have
consistent environment with the latest security fixes applied, one that allows to reproduce CI failures
and make sure that they are fixed without actually pushing them to CI. It avoids "works for me syndrome"
and enables similar development/test experience for various developers (from casual to seasoned ones) using
different platforms for development.
