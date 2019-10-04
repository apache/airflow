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
# Contributing

Contributions are welcome and are greatly appreciated! Every
little bit helps, and credit will always be given.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Types of Contributions](#types-of-contributions)
  - [Report Bugs](#report-bugs)
  - [Fix Bugs](#fix-bugs)
  - [Implement Features](#implement-features)
  - [Improve Documentation](#improve-documentation)
  - [Submit Feedback](#submit-feedback)
- [Documentation](#documentation)
- [Development environments](#development-environments)
  - [Local virtualenv development environment](#local-virtualenv-development-environment)
  - [Breeze development environment](#breeze-development-environment)
- [Pylint checks](#pylint-checks)
- [Pre-commit hooks](#pre-commit-hooks)
  - [Installing pre-commit hooks](#installing-pre-commit-hooks)
  - [Docker images for pre-commit hooks](#docker-images-for-pre-commit-hooks)
  - [Prerequisites for pre-commit hooks](#prerequisites-for-pre-commit-hooks)
  - [Pre-commit hooks installed](#pre-commit-hooks-installed)
  - [Using pre-commit hooks](#using-pre-commit-hooks)
  - [Skipping pre-commit hooks](#skipping-pre-commit-hooks)
  - [Advanced pre-commit usage](#advanced-pre-commit-usage)
- [Pull Request Guidelines](#pull-request-guidelines)
- [Testing on Travis CI](#testing-on-travis-ci)
  - [Travis CI GitHub App (new version)](#travis-ci-github-app-new-version)
  - [Travis CI GitHub Services (legacy version)](#travis-ci-github-services-legacy-version)
  - [Prefer travis-ci.com over travis-ci.org](#prefer-travis-cicom-over-travis-ciorg)
- [Changing the Metadata Database](#changing-the-metadata-database)
- [Setting up the node / npm javascript environment](#setting-up-the-node--npm-javascript-environment)
  - [Node/npm versions](#nodenpm-versions)
  - [Using npm to generate bundled files](#using-npm-to-generate-bundled-files)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Types of Contributions

## Report Bugs

Report bugs through [Apache Jira](https://issues.apache.org/jira/browse/AIRFLOW)

Please report relevant information and preferably code that exhibits
the problem.

## Fix Bugs

Look through the Jira issues for bugs. Anything is open to whoever wants
to implement it.

## Implement Features

Look through the [Apache Jira](https://issues.apache.org/jira/browse/AIRFLOW) for features.
Any unassigned "Improvement" issue is open to whoever wants to implement it.

We've created the operators, hooks, macros and executors we needed, but we
made sure that this part of Airflow is extensible. New operators,
hooks, macros and executors are very welcomed!

## Improve Documentation

Airflow could always use better documentation,
whether as part of the official Airflow docs,
in docstrings, `docs/*.rst` or even on the web as blog posts or
articles.

## Submit Feedback

The best way to send feedback is to open an issue on
[Apache Jira](https://issues.apache.org/jira/browse/AIRFLOW)

If you are proposing a feature:

- Explain in detail how it would work.
- Keep the scope as narrow as possible, to make it easier to implement.
- Remember that this is a volunteer-driven project, and that contributions are welcome :)

# Documentation

The latest API documentation is usually available
[here](https://airflow.apache.org/). To generate a local version,
you need to have set up an Airflow development environment (see below). Also
install the `doc` extra.

```
pip install -e '.[doc]'
```

Generate and serve the documentation by running:

```
cd docs
./build.sh
./start_doc_server.sh
```

# Development environments

There are two development environments you can use to develop Apache Airflow:

The first is Local  virtualenv development environment that can be used to use your IDE and to
run basic unit tests.

##  Local virtualenv development environment

All details about using and running local virtualenv enviroment for Airflow can be found
in [LOCAL_VIRTUALENV.rst](LOCAL_VIRTUALENV.rst)

It is **STRONGLY** encouraged to also install and use [Pre commit hooks](#pre-commit-hooks) for your local
development environment. Pre-commit hooks can speed up your development cycle a lot.

Advantage of local installation is that you have packages installed locally. You do not have to
enter Docker/container environment and you can easily debug the code locally.
You can also have access to python virtualenv that contains the necessary requirements
and use it in your local IDE - this aids autocompletion, and run tests directly from within the IDE.

The disadvantage is that you have to maintain your dependencies and local environment consistent with
other development environments that you have on your local machine.

Another disadvantage is that you cannot run tests that require
external components - mysql, postgres database, hadoop, mongo, cassandra, redis etc..
The tests in Airflow are a mixture of unit and integration tests and some of them
require those components to be setup. Only real unit tests can be run by default in local environment.

If you want to run integration tests, you can technically configure and install the dependencies on your own,
but it is usually complex and it's better to use
[Breeze development environment](#breeze-development-environment) instead.

Yet another disadvantage of using local virtualenv is that it is very difficult to make sure that your
local environment is consistent with other developer's environments. This can often lead to "works for me"
syndrome. The Breeze development environment provides reproducible environment that is
consistent with other developers.

## Breeze development environment

All details about using and running Airflow Breeze can be found in [BREEZE.rst](BREEZE.rst)

Using Breeze locally is easy. It's called **Airflow Breeze** as in "_It's a Breeze to develop Airflow_"

The advantage of the Airflow Breeze environment is that it is a full environment
including external components - mysql database, hadoop, mongo, cassandra, redis etc. Some of the tests in
Airflow require those external components. The Breeze environment provides preconfigured docker compose
environment with all those services are running and can be used by tests automatically.

Another advantage is that the Airflow Breeze environment is pretty much the same
as used in [Travis CI](https://travis-ci.com/) automated builds, and if the tests run in
your local environment they will most likely work in CI as well.

The disadvantage of Airflow Breeze is that it is fairly complex and requires time to setup. However it is all
automated and easy to setup. Another disadvantage is that it takes a lot of space in your local Docker cache.
There is a separate environment for different python versions and airflow versions and each of the images take
around 3GB in total. Building and preparing the environment by default uses pre-built images from DockerHub
(requires time to download and extract those GB of images) and less than 10 minutes per python version
to build.

The environment for Breeze runs in the background taking precious resources - disk space and CPU. You
can stop the environment manually after you use it or even use `bare` environment to decrease resource
usage.

Note that the CI images are not supposed to be used in production environments. They are optimised
for repeatability of tests, maintainability and speed of building rather than production performance.
The production images are not yet available (but they will be).

# Pylint checks

Note that for pylint we are in the process of fixing pylint code checks for the whole Airflow code. This is
a huge task so we implemented an incremental approach for the process. Currently most of the code is
excluded from pylint checks via [scripts/ci/pylint_todo.txt](scripts/ci/pylint_todo.txt). We have an open JIRA
issue [AIRFLOW-4364](https://issues.apache.org/jira/browse/AIRFLOW-4364) which has a number of
sub-tasks for each of the modules that should be made compatible. Fixing pylint problems is one of
straightforward and easy tasks to do (but time-consuming) so if you are a first-time contributor to
Airflow you can choose one of the sub-tasks as your first issue to fix. The process to fix the issue looks
as follows:

1) Remove module/modules from the [scripts/ci/pylint_todo.txt](scripts/ci/pylint_todo.txt)
2) Run [scripts/ci/ci_pylint.sh](scripts/ci/ci_pylint.sh)
3) Fix all the issues reported by pylint
4) Re-run [scripts/ci/ci_pylint.sh](scripts/ci/ci_pylint.sh)
5) If you see "success" - submit PR following [Pull Request guidelines](#pull-request-guidelines)
6) You can refresh periodically [scripts/ci/pylint_todo.txt](scripts/ci/pylint_todo.txt) file.
   You can do it by running
   [scripts/ci/ci_refresh_pylint_todo.sh](scripts/ci/ci_refresh_pylint_todo.sh).
   This can take quite some time (especially on MacOS)!

You can follow these guidelines when fixing pylint errors:

* Ideally fix the errors rather than disable pylint checks - often you can easily refactor the code
  (IntelliJ/PyCharm might be helpful when extracting methods in complex code or moving methods around)
* When disabling particular problem - make sure to disable only that error-via the symbolic name
  of the error as reported by pylint
* If there is a single line where to disable particular error you can add comment following the line
  that causes the problem. For example:
```python
def MakeSummary(pcoll, metric_fn, metric_keys):  # pylint: disable=invalid-name
```
* When there are multiple lines/block of code to disable an error you can surround the block with
  comment only pylint:disable/pylint:enable lines. For example:

```python
# pylint: disable=too-few-public-methods
class LoginForm(Form):
    """Form for the user"""
    username = StringField('Username', [InputRequired()])
    password = PasswordField('Password', [InputRequired()])
# pylint: enable=too-few-public-methods
```

# Pre-commit hooks

Pre-commit hooks are fantastic way of speeding up your local development cycle. Those pre-commit checks will
only check the files that you are currently working on which make them fast. Yet they are using exactly
the same environment as the CI checks are using, so you can be pretty sure your modifications
will be ok for CI if they pass pre-commit checks.

You are *STRONGLY* encouraged to install pre-commit hooks as they speed up your development and place less
burden on the CI infrastructure.

We have integrated the fantastic [pre-commit](https://pre-commit.com/) framework in our development workflow.
You need to have python 3.6 installed in your host in order to install and use it. It's best to run your
commits when you have your local virtualenv for Airflow activated (then pre-commit and other
dependencies are automatically installed). You can also install pre-commit manually using `pip install`.

The pre-commit hooks require Docker Engine to be configured as the static checks static checks are
executed in docker environment. You should build the images locally before installing pre-commit checks as
described in [Breeze](BREEZE.rst). In case you do not have your local images built
the pre-commit hooks fail and provide instructions on what needs to be done.

## Installing pre-commit hooks

```
pre-commit install
```

Running the command by default turns on pre-commit checks for `commit` operations in git.

You can also decide to install the checks also for `pre-push` operation:

```
pre-commit install -t pre-push
```

You can see advanced usage of the install method via

```
pre-commit install --help
```

## Docker images for pre-commit hooks

Before running the pre-commit hooks you must first build the docker images as described in
[BREEZE](BREEZE.rst).

Sometimes your image is outdated (when dependencies change) and needs to be rebuilt because some
dependencies have been changed. In such case the docker build pre-commit will inform
you that you should build the image.

## Prerequisites for pre-commit hooks

The pre-commit hooks use several external linters that need to be installed before pre-commit are run.
Each of the checks install its own environment, so you do not need to install those, but there are some
checks that require locally installed binaries. In Linux you typically install them
with `sudo apt install` on MacOS with `brew install`.

The current list of prerequisites:

* `xmllint`: Linux - install via `sudo apt install xmllint`, MacOS - install via `brew install xmllint`

## Pre-commit hooks installed

In airflow we have the following checks:

```text
check-hooks-apply                Check hooks apply to the repository
check-apache-license             Checks compatibility with Apache License requirements
check-merge-conflict             Checks if merge conflict is being committed
check-executables-have-shebangs  Check that executables have shebang
check-xml                        Check XML files with xmllint
doctoc                           Refresh table-of-contents for md files
detect-private-key               Detects if private key is added to the repository
end-of-file-fixer                Make sure that there is an empty line at the end
flake8                           Run flake8
forbid-tabs                      Fails if tabs are used in the project
insert-license                   Add licences for most file types
lint-dockerfile                  Lint dockerfile
mixed-line-ending                Detects if mixed line ending is used (\r vs. \r\n)
mypy                             Run mypy
pylint                           Run pylint
shellcheck                       Check shell files with shellcheck
yamllint                         Check yaml files with yamllint
```

The check-apache-licence check is normally skipped for commits unless `.pre-commit-config.yaml` file
is changed. This check always run for the full set of files and if you want to run it locally you need to
specify `--all-files` flag of pre-commit. For example:

`pre-commit run check-apache-licenses --all-files`

## Using pre-commit hooks

After installing pre-commit hooks are run automatically when you commit the code, but you can
run pre-commit hooks manually as needed.

*You can run all checks on your staged files by running:*
`pre-commit run`

*You can run only one mypy check on your staged files by running:*
`pre-commit run mypy`

*You can run only one mypy checks manually on all files by running:*
`pre-commit run mypy --all-files`

*You can run all checks manually on all files by running:*
`SKIP=pylint pre-commit run --all-files`

*You can skip one or more of the checks by specifying comma-separated list of checks to skip in SKIP variable:*
`SKIP=pylint,mypy pre-commit run --all-files`

*You can run only one mypy checks manually on a single file (or list of files) by running:*
`pre-commit run mypy --files PATH_TO_FILE`. Example: `pre-commit run mypy --files airflow/operators/bash_operator.py`

## Skipping pre-commit hooks

You can always skip running the tests by providing `--no-verify` flag to `git commit` command.

## Advanced pre-commit usage

You can check other usages of pre-commit framework at [Pre-commit website](https://pre-commit.com/)

# Pull Request Guidelines

Before you submit a pull request from your forked repo, check that it
meets these guidelines:

1. The pull request should include tests, either as doctests, unit tests, or both. The airflow repo uses
[Travis CI](https://travis-ci.org/apache/airflow) to run the tests and
[codecov](https://codecov.io/gh/apache/airflow) to track coverage.
You can set up both for free on your fork (see "Testing on Travis CI" section below).
It will help you make sure you do not break the build with your PR and that you help increase coverage.
1. Please [rebase your fork](http://stackoverflow.com/a/7244456/1110993), squash commits, and
resolve all conflicts.
1. Every pull request should have an associated
[JIRA](https://issues.apache.org/jira/browse/AIRFLOW/?selectedTab=com.atlassian.jira.jira-projects-plugin:summary-panel).
The JIRA link should also be contained in the PR description.
1. Preface your commit's subject & PR's title with **[AIRFLOW-XXX]** where *XXX* is the JIRA number.
We compose release notes (i.e. for Airflow releases) from all commit titles in a release.
By placing the JIRA number in the commit title and hence in the release notes, Airflow users can look into
JIRA and GitHub PRs for more details about a particular change.
1. Add an [Apache License](http://www.apache.org/legal/src-headers.html) header to all new files
1. If the pull request adds functionality, the docs should be updated as part of the same PR. Doc string
are often sufficient.  Make sure to follow the Sphinx compatible standards.
1. The pull request should work for Python 3.5 and 3.6.
1. As Airflow grows as a project, we try to enforce a more consistent style and try to follow the Python
community guidelines. We currently enforce most [PEP8](https://www.python.org/dev/peps/pep-0008/) and a
few other linting rules - described in [Running static code analysis locally](#running-static-code-analysis-locally).
It's a good idea to run tests locally before opening PR.
1. Please read this excellent [article](http://chris.beams.io/posts/git-commit/) on commit messages and
adhere to them. It makes the lives of those who come after you a lot easier.

# Testing on Travis CI

We currently rely heavily on Travis CI for running the full Airflow test suite
as running all of the tests locally requires significant setup.  You can setup
Travis CI in your fork of Airflow by following the
[Travis CI Getting Started guide][travis-ci-getting-started].

There are two different options available for running Travis CI which are
setup as separate components on GitHub:

1. **Travis CI GitHub App** (new version)
1. **Travis CI GitHub Services** (legacy version)

## Travis CI GitHub App (new version)

1. Once installed, you can configure the Travis CI GitHub App at
https://github.com/settings/installations -> Configure Travis CI.

1. For the Travis CI GitHub App, you can set repository access to either "All
repositories" for convenience, or "Only select repositories" and choose
`<username>/airflow` in the dropdown.

1. You can access Travis CI for your fork at
`https://travis-ci.com/<username>/airflow`.

## Travis CI GitHub Services (legacy version)

The Travis CI GitHub Services versions uses an Authorized OAuth App.  Note
that `apache/airflow` is currently still using the legacy version.

1. Once installed, you can configure the Travis CI Authorized OAuth App at
https://github.com/settings/connections/applications/88c5b97de2dbfc50f3ac.

1. If you are a GitHub admin, click the "Grant" button next to your
organization; otherwise, click the "Request" button.

1. For the Travis CI Authorized OAuth App, you may have to grant access to the
forked `<organization>/airflow` repo even though it is public.

1. You can access Travis CI for your fork at
`https://travis-ci.org/<organization>/airflow`.

## Prefer travis-ci.com over travis-ci.org

The travis-ci.org site for open source projects is now legacy and new projects
should instead be created on travis-ci.com for both private repos and open
source.

Note that there is a second Authorized OAuth App available called "Travis CI
for Open Source" used for the
[legacy travis-ci.org service][travis-ci-org-vs-com].  It should not be used
for new projects.

More information:

- [Open Source on travis-ci.com][travis-ci-open-source]
- [Legacy GitHub Services to GitHub Apps Migration Guide][travis-ci-migrating]
- [Migrating Multiple Repositories to GitHub Apps Guide][travis-ci-migrating-2]

[travis-ci-getting-started]: https://docs.travis-ci.com/user/getting-started/
[travis-ci-migrating-2]: https://docs.travis-ci.com/user/travis-migrate-to-apps-gem-guide/
[travis-ci-migrating]: https://docs.travis-ci.com/user/legacy-services-to-github-apps-migration-guide/
[travis-ci-open-source]: https://docs.travis-ci.com/user/open-source-on-travis-ci-com/
[travis-ci-org-vs-com]: https://devops.stackexchange.com/a/4305/8830

# Changing the Metadata Database

When developing features the need may arise to persist information to the the
metadata database. Airflow has [Alembic](https://bitbucket.org/zzzeek/alembic)
built-in to handle all schema changes. Alembic must be installed on your
development machine before continuing.

```
# starting at the root of the project
$ pwd
~/airflow
# change to the airflow directory
$ cd airflow
$ alembic revision -m "add new field to db"
  Generating
~/airflow/airflow/migrations/versions/12341123_add_new_field_to_db.py
```

# Setting up the node / npm javascript environment

`airflow/www/` contains all npm-managed, front end assets.
Flask-Appbuilder itself comes bundled with jQuery and bootstrap.
While these may be phased out over time, these packages are currently not
managed with npm.

## Node/npm versions

Make sure you are using recent versions of node and npm. No problems have been found with node>=8.11.3 and
npm>=6.1.3

## Using npm to generate bundled files

### npm

First, npm must be available in your environment. If you are on Mac and it is not installed,
you can run the following commands (taken from [this source](https://gist.github.com/DanHerbert/9520689)):

```
brew install node --without-npm
echo prefix=~/.npm-packages >> ~/.npmrc
curl -L https://www.npmjs.com/install.sh | sh
```

The final step is to add `~/.npm-packages/bin` to your `PATH` so commands you install globally are usable.
Add something like this to your `.bashrc` file, then `source ~/.bashrc` to reflect the change.

```
export PATH="$HOME/.npm-packages/bin:$PATH"
```

You can also follow
[the general npm installation instructions](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm).

### npm packages

To install third party libraries defined in `package.json`, run the
following within the `airflow/www/` directory which will install them in a
new `node_modules/` folder within `www/`.

```bash
# from the root of the repository, move to where our JS package.json lives
cd airflow/www/
# run npm install to fetch all the dependencies
npm install
```

To parse and generate bundled files for airflow, run either of the
following commands. The `dev` flag will keep the npm script running and
re-run it upon any changes within the assets directory.

```
# Compiles the production / optimized js & css
npm run prod

# Start a web server that manages and updates your assets as you modify them
npm run dev
```

### Upgrading npm packages

Should you add or upgrade an npm package, which involves changing `package.json`, you'll need to re-run `npm install`
and push the newly generated `package-lock.json` file so we get the reproducible build.

### Javascript Style Guide

We try to enforce a more consistent style and try to follow the JS community guidelines.
Once you add or modify any javascript code in the project, please make sure it follows the guidelines
defined in [Airbnb JavaScript Style Guide](https://github.com/airbnb/javascript).
Apache Airflow uses [ESLint](https://eslint.org/) as a tool for identifying and reporting on patterns
in JavaScript, which can be used by running any of the following commands.

```bash
# Check JS code in .js and .html files, and report any errors/warnings
npm run lint

# Check JS code in .js and .html files, report any errors/warnings and fix them if possible
npm run lint:fix
```
