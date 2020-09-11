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
# Apache Airflow

[![PyPI version](https://badge.fury.io/py/apache-airflow.svg)](https://badge.fury.io/py/apache-airflow)
[![GitHub Build](https://github.com/apache/airflow/workflows/CI%20Build/badge.svg)](https://github.com/apache/airflow/actions)
[![Coverage Status](https://img.shields.io/codecov/c/github/apache/airflow/master.svg)](https://codecov.io/github/apache/airflow?branch=master)
[![Documentation Status](https://readthedocs.org/projects/airflow/badge/?version=latest)](https://airflow.readthedocs.io/en/latest/?badge=latest)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/apache-airflow.svg)](https://pypi.org/project/apache-airflow/)
[![Docker Pulls](https://img.shields.io/docker/pulls/apache/airflow.svg)](https://hub.docker.com/r/apache/airflow)
[![Docker Stars](https://img.shields.io/docker/stars/apache/airflow.svg)](https://hub.docker.com/r/apache/airflow)

[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheAirflow.svg?style=social&label=Follow)](https://twitter.com/ApacheAirflow)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://s.apache.org/airflow-slack)

[Apache Airflow](https://airflow.apache.org/docs/stable/) (or simply Airflow) is a platform to programmatically author, schedule, and monitor workflows.

When workflows are defined as code, they become more maintainable, versionable, testable, and collaborative.

Use Airflow to author workflows as directed acyclic graphs (DAGs) of tasks. The Airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of contents**

- [Requirements](#requirements)
- [Getting started](#getting-started)
- [Installing from PyPI](#installing-from-pypi)
- [Official source code](#official-source-code)
- [Project Focus](#project-focus)
- [Principles](#principles)
- [User Interface](#user-interface)
- [Backport packages](#backport-packages)
- [Contributing](#contributing)
- [Who uses Apache Airflow?](#who-uses-apache-airflow)
- [Who Maintains Apache Airflow?](#who-maintains-apache-airflow)
- [Can I use the Apache Airflow logo in my presentation?](#can-i-use-the-apache-airflow-logo-in-my-presentation)
- [Airflow merchandise](#airflow-merchandise)
- [Links](#links)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Requirements

Apache Airflow is tested with:

|              | Master version (2.0.0dev) | Stable version (1.10.12) |
| ------------ | ------------------------- | ------------------------ |
| Python       | 3.6, 3.7, 3.8             | 2.7, 3.5, 3.6, 3.7, 3.8  |
| PostgreSQL   | 9.6, 10                   | 9.6, 10                  |
| MySQL        | 5.7                       | 5.6, 5.7                 |
| SQLite       | latest stable             | latest stable            |
| Kubernetes   | 1.16.2, 1.17.0            | 1.16.2, 1.17.0           |

> Note: SQLite is used primarily for development purpose.

### Additional notes on Python version requirements

* Stable version [requires](https://github.com/apache/airflow/issues/8162) at least Python 3.5.3 when using Python 3

## Getting started

Visit the official Airflow website documentation (latest **stable** release) for help with [installing Airflow](https://airflow.apache.org/installation.html), [getting started](https://airflow.apache.org/start.html), or walking through a more complete [tutorial](https://airflow.apache.org/tutorial.html).

> Note: If you're looking for documentation for master branch (latest development branch): you can find it on [ReadTheDocs](https://airflow.readthedocs.io/en/latest/).

For more information on Airflow's Roadmap or Airflow Improvement Proposals (AIPs), visit the [Airflow Wiki](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Home).

Official Docker (container) images for Apache Airflow are described in [IMAGES.rst](IMAGES.rst).

## Installing from PyPI

Airflow is published as `apache-airflow` package in PyPI. Installing it however might be sometimes tricky
because Airflow is a bit of both a library and application. Libraries usually keep their dependencies open and
applications usually pin them, but we should do neither and both at the same time. We decided to keep
our dependencies as open as possible (in `setup.py`) so users can install different versions of libraries
if needed. This means that from time to time plain `pip install apache-airflow` will not work or will
produce unusable Airflow installation.

In order to have repeatable installation, however, introduced in **Airflow 1.10.10** and updated in
**Airflow 1.10.12** we also keep a set of "known-to-be-working" constraint files in the
orphan `constraints-master` and `constraints-1-10` branches. We keep those "known-to-be-working"
constraints files separately per major/minor python version.
You can use them as constraint files when installing Airflow from PyPI. Note that you have to specify
correct Airflow tag/version/branch and python versions in the URL.

1. Installing just airflow:

```bash
pip install apache-airflow==1.10.12 \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.7.txt"
```

2. Installing with extras (for example postgres,google)
```bash
pip install apache-airflow[postgres,google]==1.10.12 \
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.7.txt"
```

## Official source code

Apache Airflow is an [Apache Software Foundation](http://www.apache.org) (ASF) project, and our official source code releases:

- Follow the [ASF Release Policy](http://www.apache.org/legal/release-policy.html)
- Can be downloaded from [the ASF Distribution Directory](https://downloads.apache.org)
- Are cryptographically signed by the release manager
- Are officially voted on by the PMC members during the [Release Approval Process](http://www.apache.org/legal/release-policy.html#release-approval)

Following the ASF rules, the source packages released must be sufficient for a user to build and test the release provided they have access to the appropriate platform and tools.

Other ways of retrieving source code are "convenience" methods. For example:

- Tagging in GitHub to mark the git project sources that were used to generate official source packages

We also have binary "convenience" packages:

- PyPI releases to install Airflow using standard python tools
- Docker Images published in the [Apache Airflow DockerHub](https://hub.docker.com/repository/docker/apache/airflow).

These artifacts are not official releases, but they are built using officially released sources.

> Note: Airflow Summit 2020's ["Production Docker Image talk"](https://youtu.be/wDr3Y7q2XoI) explains context, architecture and customization/extension methods.

## Project Focus

Airflow works best with workflows that are mostly static and slowly changing. When the structure is similar from one run to the next, it allows for clarity around unit of work and continuity. Other similar projects include [Luigi](https://github.com/spotify/luigi), [Oozie](http://oozie.apache.org/) and [Azkaban](https://azkaban.github.io/).

Airflow is commonly used to process data, but has the opinion that tasks should ideally be idempotent, and should not pass large quantities of data from one task to the next (though tasks can pass metadata using Airflow's [Xcom feature](https://airflow.apache.org/docs/stable/concepts.html#xcoms)). For high-volume, data-intensive tasks, a best practice is to delegate to external services that specialize on that type of work.

Airflow **is not** a streaming solution. Airflow is not in the [Spark Streaming](http://spark.apache.org/streaming/) or [Storm](https://storm.apache.org/) space.

## Principles

- **Dynamic**:  Airflow pipelines are configuration as code (Python), allowing for dynamic pipeline generation. This allows for writing code that instantiates pipelines dynamically.
- **Extensible**:  Easily define your own operators, executors and extend the library so that it fits the level of abstraction that suits your environment.
- **Elegant**:  Airflow pipelines are lean and explicit. Parameterizing your scripts is built into the core of Airflow using the powerful **Jinja** templating engine.
- **Scalable**:  Airflow has a modular architecture and uses a message queue to orchestrate an arbitrary number of workers.

## User Interface

- **DAGs**: Overview of all DAGs in your environment.

  ![](/docs/img/dags.png)

- **Tree View**: Tree representation of a DAG that spans across time.

  ![](/docs/img/tree.png)

- **Graph View**: Visualization of a DAG's dependencies and their current status for a specific run.

  ![](/docs/img/graph.png)

- **Task Duration**: Total time spent on different tasks over time.

  ![](/docs/img/duration.png)

- **Gantt View**: Duration and overlap of a DAG.

  ![](/docs/img/gantt.png)

- **Code View**:  Quick way to view source code of a DAG.

  ![](/docs/img/code.png)


## Backport packages

### Context: Airflow 2.0 operators, hooks, and secrets

Currently, stable Apache Airflow versions are from the 1.10.* series.
We are working on the future, major version of Airflow from the 2.0.* series.
It is going to be released in 2020. However, the exact time of release depends on many factors and is
not yet confirmed.

We have already a lot of changes in the operators, transfers, hooks, sensors, secrets for many external
systems, but they are not used nor tested widely because they are part of the master/2.0 release.

In the Airflow 2.0 - following AIP-21 "change in import paths" all the non-core interfaces to external
systems of Apache Airflow have been moved to the "airflow.providers" package.

Thanks to that and automated backport effort we took, the operators from Airflow 2.0
can be used in Airflow 1.10 as separately installable packages, with the constraint that
those packages can only be used in python3.6+ environment.

### Installing Airflow 2.0 operators in Airflow 1.10

We released backport packages that can be installed for older Airflow versions.
Those backport packages are going to be released more frequently that main Airflow 1.10.* releases.

You will not have to upgrade your Airflow version to use those packages. You can find those packages in the
[PyPI](https://pypi.org/search/?q=apache-airflow-backport-providers&o=) and install them separately for each
provider.

Those packages are available now and can be used in the latest Airflow 1.10.* version. Most of those
packages are also installable and usable in most Airflow 1.10.* releases but there is no extensive testing
done beyond the latest released version, so you might expect more problems in earlier Airflow versions.

### An easier migration path to 2.0

With backported providers package users can migrate their DAGs to the new providers package incrementally
and once they convert to the new operators/sensors/hooks they can seamlessly migrate their
environments to Airflow 2.0. The nice thing about providers backport packages is that you can use
both old and new classes at the same time - even in the same DAG. So your migration can be gradual and smooth.
Note that in Airflow 2.0 old classes raise deprecation warning and redirect to the new classes wherever
it is possible. In some rare cases the new operators will not be fully backwards compatible - you will find
information about those cases in [UPDATING.md](UPDATING.md) where we explained all such cases. Switching
early to the Airflow 2.0 operators while still running Airflow 1.10 will make your migration much easier.

More information about the status and releases of the back-ported packages are available
at [Backported providers package page](https://cwiki.apache.org/confluence/display/AIRFLOW/Backported+providers+packages+for+Airflow+1.10.*+series)


### Installing backport packages

Note that the backport packages might require extra dependencies. Pip installs the required dependencies
automatically when it installs the backport package, but there are sometimes cross-dependencies between the
backport packages. For example `google` package has cross-dependency with `amazon` package to allow
transfers between those two cloud providers. You might need to install those packages in case you use
cross-dependent packages. The easiest way to install them is to use "extras" when installing the package,
for example the below will install both `google` and `amazon` backport packages:

```bash
pip install apache-airflow-backport-providers-google[amazon]
```

This is all documented in the PyPI description of the packages
as well as in the README.md file available for each provider package. For example for google package
you can find the readme in [README.md](airflow/providers/google/README.md). You will also find there
the summary of both - new classes and moved classes as well as requirement information.

### Troubleshooting installing backport packages

Backport providers only work when they are installed in the same namespace as the 'apache-airflow' 1.10
package. This is majority of cases when you simply run `pip install` - it installs all packages
in the same folder (usually in `/usr/local/lib/pythonX.Y/site-packages`). But when you install
the `apache-airflow` and `apache-airflow-backport-package-*` using different methods (for example using
`pip install -e .` or `pip install --user` they might be installed in different namespaces.
If that's the case, the provider packages will not be importable (the error in such case is
`ModuleNotFoundError: No module named 'airflow.providers'`).

If you experience the problem, you can easily fix it by creating symbolic link
in your installed "airflow" folder to the  "providers" folder where you installed your backport packages.
If you installed it with `-e`, this link should be created in your airflow
sources,  if you installed it with the `--user` flag it should be from the
`~/.local/lib/pythonX.Y/site-packages/airflow/` folder,

## Contributing

Want to help build Apache Airflow? Check out our [contributing documentation](https://github.com/apache/airflow/blob/master/CONTRIBUTING.rst).

## Who uses Apache Airflow?

More than 350 organizations are using Apache Airflow [in the wild](https://github.com/apache/airflow/blob/master/INTHEWILD.md).

## Who Maintains Apache Airflow?

Airflow is the work of the [community](https://github.com/apache/airflow/graphs/contributors),
but the [core committers/maintainers](https://people.apache.org/committers-by-project.html#airflow)
are responsible for reviewing and merging PRs as well as steering conversation around new feature requests.
If you would like to become a maintainer, please review the Apache Airflow
[committer requirements](https://cwiki.apache.org/confluence/display/AIRFLOW/Committers).

## Can I use the Apache Airflow logo in my presentation?

Yes! Be sure to abide by the Apache Foundation [trademark policies](https://www.apache.org/foundation/marks/#books) and the Apache Airflow [Brandbook](https://cwiki.apache.org/confluence/display/AIRFLOW/Brandbook). The most up to date logos are found in [this repo](/docs/img/logos) and on the Apache Software Foundation [website](https://www.apache.org/logos/about.html).

## Airflow merchandise

If you would love to have Apache Airflow stickers, t-shirt etc. then check out
[Redbubble Shop](https://www.redbubble.com/i/sticker/Apache-Airflow-by-comdev/40497530.EJUG5).

## Links

- [Documentation](https://airflow.apache.org/docs/stable/)
- [Chat](https://s.apache.org/airflow-slack)
- [More](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Links)
