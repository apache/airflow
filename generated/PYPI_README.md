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

<!-- PLEASE DO NOT MODIFY THIS FILE. IT HAS BEEN GENERATED AUTOMATICALLY FROM THE `README.md` FILE OF THE
PROJECT BY THE `generate-pypi-readme` PRE-COMMIT. YOUR CHANGES HERE WILL BE AUTOMATICALLY OVERWRITTEN.-->

# Apache Airflow

[![PyPI version](https://badge.fury.io/py/apache-airflow.svg)](https://badge.fury.io/py/apache-airflow)
[![GitHub Build](https://github.com/apache/airflow/workflows/Tests/badge.svg)](https://github.com/apache/airflow/actions)
[![Coverage Status](https://codecov.io/gh/apache/airflow/graph/badge.svg?token=WdLKlKHOAU)](https://codecov.io/gh/apache/airflow)
[![License](https://img.shields.io/:license-Apache%202-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/apache-airflow.svg)](https://pypi.org/project/apache-airflow/)
[![Docker Pulls](https://img.shields.io/docker/pulls/apache/airflow.svg)](https://hub.docker.com/r/apache/airflow)
[![Docker Stars](https://img.shields.io/docker/stars/apache/airflow.svg)](https://hub.docker.com/r/apache/airflow)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/apache-airflow)](https://pypi.org/project/apache-airflow/)
[![Artifact HUB](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/apache-airflow)](https://artifacthub.io/packages/search?repo=apache-airflow)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheAirflow.svg?style=social&label=Follow)](https://twitter.com/ApacheAirflow)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://s.apache.org/airflow-slack)
[![Contributors](https://img.shields.io/github/contributors/apache/airflow)](https://github.com/apache/airflow/graphs/contributors)
[![OSSRank](https://shields.io/endpoint?url=https://ossrank.com/shield/6)](https://ossrank.com/p/6)

<picture width="500">
  <img
    src="https://github.com/apache/airflow/blob/19ebcac2395ef9a6b6ded3a2faa29dc960c1e635/docs/apache-airflow/img/logos/wordmark_1.png?raw=true"
    alt="Apache Airflow logo"
  />
</picture>

[Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/) (or simply Airflow) is a platform to programmatically author, schedule, and monitor workflows.

When workflows are defined as code, they become more maintainable, versionable, testable, and collaborative.

Use Airflow to author workflows as directed acyclic graphs (DAGs) of tasks. The Airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

## Requirements

Apache Airflow is tested with:

|            | Main version (dev)         | Stable version (2.10.2)    |
|------------|----------------------------|----------------------------|
| Python     | 3.8, 3.9, 3.10, 3.11, 3.12 | 3.8, 3.9, 3.10, 3.11, 3.12 |
| Platform   | AMD64/ARM64(\*)            | AMD64/ARM64(\*)            |
| Kubernetes | 1.28, 1.29, 1.30, 1.31     | 1.27, 1.28, 1.29, 1.30     |
| PostgreSQL | 12, 13, 14, 15, 16, 17     | 12, 13, 14, 15, 16         |
| MySQL      | 8.0, 8.4, Innovation       | 8.0, 8.4, Innovation       |
| SQLite     | 3.15.0+                    | 3.15.0+                    |

\* Experimental

**Note**: MySQL 5.x versions are unable to or have limitations with
running multiple schedulers -- please see the [Scheduler docs](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/scheduler.html).
MariaDB is not tested/recommended.

**Note**: SQLite is used in Airflow tests. Do not use it in production. We recommend
using the latest stable version of SQLite for local development.

**Note**: Airflow currently can be run on POSIX-compliant Operating Systems. For development, it is regularly
tested on fairly modern Linux Distros and recent versions of macOS.
On Windows you can run it via WSL2 (Windows Subsystem for Linux 2) or via Linux Containers.
The work to add Windows support is tracked via [#10388](https://github.com/apache/airflow/issues/10388), but
it is not a high priority. You should only use Linux-based distros as "Production" execution environment
as this is the only environment that is supported. The only distro that is used in our CI tests and that
is used in the [Community managed DockerHub image](https://hub.docker.com/p/apache/airflow) is
`Debian Bookworm`.

## Getting started

Visit the official Airflow website documentation (latest **stable** release) for help with
[installing Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/),
[getting started](https://airflow.apache.org/docs/apache-airflow/stable/start.html), or walking
through a more complete [tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/).

> Note: If you're looking for documentation for the main branch (latest development branch): you can find it on [s.apache.org/airflow-docs](https://s.apache.org/airflow-docs/).

For more information on Airflow Improvement Proposals (AIPs), visit
the [Airflow Wiki](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals).

Documentation for dependent projects like provider packages, Docker image, Helm Chart, you'll find it in [the documentation index](https://airflow.apache.org/docs/).

## Installing from PyPI

We publish Apache Airflow as `apache-airflow` package in PyPI. Installing it however might be sometimes tricky
because Airflow is a bit of both a library and application. Libraries usually keep their dependencies open, and
applications usually pin them, but we should do neither and both simultaneously. We decided to keep
our dependencies as open as possible (in `pyproject.toml`) so users can install different versions of libraries
if needed. This means that `pip install apache-airflow` will not work from time to time or will
produce unusable Airflow installation.

To have repeatable installation, however, we keep a set of "known-to-be-working" constraint
files in the orphan `constraints-main` and `constraints-2-0` branches. We keep those "known-to-be-working"
constraints files separately per major/minor Python version.
You can use them as constraint files when installing Airflow from PyPI. Note that you have to specify
correct Airflow tag/version/branch and Python versions in the URL.


1. Installing just Airflow:

> Note: Only `pip` installation is currently officially supported.

While it is possible to install Airflow with tools like [Poetry](https://python-poetry.org) or
[pip-tools](https://pypi.org/project/pip-tools), they do not share the same workflow as
`pip` - especially when it comes to constraint vs. requirements management.
Installing via `Poetry` or `pip-tools` is not currently supported.

There are known issues with ``bazel`` that might lead to circular dependencies when using it to install
Airflow. Please switch to ``pip`` if you encounter such problems. ``Bazel`` community works on fixing
the problem in `this PR <https://github.com/bazelbuild/rules_python/pull/1166>`_ so it might be that
newer versions of ``bazel`` will handle it.

If you wish to install Airflow using those tools, you should use the constraint files and convert
them to the appropriate format and workflow that your tool requires.


```bash
pip install 'apache-airflow==2.10.2' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.8.txt"
```

2. Installing with extras (i.e., postgres, google)

```bash
pip install 'apache-airflow[postgres,google]==2.10.2' \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.8.txt"
```

For information on installing provider packages, check
[providers](http://airflow.apache.org/docs/apache-airflow-providers/index.html).

## Official source code

Apache Airflow is an [Apache Software Foundation](https://www.apache.org) (ASF) project,
and our official source code releases:

- Follow the [ASF Release Policy](https://www.apache.org/legal/release-policy.html)
- Can be downloaded from [the ASF Distribution Directory](https://downloads.apache.org/airflow)
- Are cryptographically signed by the release manager
- Are officially voted on by the PMC members during the
  [Release Approval Process](https://www.apache.org/legal/release-policy.html#release-approval)

Following the ASF rules, the source packages released must be sufficient for a user to build and test the
release provided they have access to the appropriate platform and tools.


## Contributing

Want to help build Apache Airflow? Check out our [contributing documentation](https://github.com/apache/airflow/blob/main/contributing-docs/README.rst).

Official Docker (container) images for Apache Airflow are described in [images](dev/breeze/doc/ci/02_images.md).


## Voting Policy

* Commits need a +1 vote from a committer who is not the author
* When we do AIP voting, both PMC member's and committer's `+1s` are considered a binding vote.

## Who uses Apache Airflow?

We know about around 500 organizations that are using Apache Airflow (but there are likely many more)
[in the wild](https://github.com/apache/airflow/blob/main/INTHEWILD.md).

If you use Airflow - feel free to make a PR to add your organisation to the list.


## Who maintains Apache Airflow?

Airflow is the work of the [community](https://github.com/apache/airflow/graphs/contributors),
but the [core committers/maintainers](https://people.apache.org/committers-by-project.html#airflow)
are responsible for reviewing and merging PRs as well as steering conversations around new feature requests.
If you would like to become a maintainer, please review the Apache Airflow
[committer requirements](https://github.com/apache/airflow/blob/main/COMMITTERS.rst#guidelines-to-become-an-airflow-committer).
