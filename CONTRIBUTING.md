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

Contributions are welcome and are greatly appreciated! Every little bit helps, and credit will always be 
given.

# Table of Contents

* [Development environment](#development-environment)
  - [Tools used](#tools-used)
  - [Setting up a development environment](#setting-up-a-development-environment)
  - [Building front-end assets](#building-front-end-assets)
* [Developing Airflow](#developing-airflow)
  - [Running unit tests](#running-unit-tests)
  - [Setting up your own Travis CI](#setting-up-your-own-travis-ci)
  - [Writing documentation](#writing-documentation)
  - [Changing the metastore schema](#changing-the-metastore-schema)
  - [Conventions](#conventions)
* [Pull request guidelines](#pull-request-guidelines)
* [Types of contributions](#types-of-contributions)
  - [Documentation](#documentation)
  - [Bug fixes & new small features](#bug-fixes--new-small-features)
  - [Large changes](#large-changes)
  - [Breaking changes & deprecation warnings](#breaking-changes--deprecation-warnings)
  - [Other feedback](#other-feedback)
* [Tool specific tips & tricks](#tool-specific-tips--tricks)
  - [reStructuredText](#restructuredtext)
  - [Pylint](#pylint)

# Development environment

This section explains how to set up a development environment for contributing to Apache Airflow.

## Tools used

We use the following tools (in the CI):

For code quality:
- [Travis](https://travis-ci.org/apache/airflow) for CI
- [Flake8](http://flake8.pycqa.org) for code linting
- [Pylint](https://www.pylint.org) for more code linting
- [Mypy](http://mypy-lang.org) for static type checking
- [Apache Rat](https://creadur.apache.org) for checking Apache license headers
- [Sphinx](http://www.sphinx-doc.org) for documentation building
- [Read the Docs](https://readthedocs.org/projects/airflow) for documentation hosting

All tools can be called separate from the CI pipeline. See `.travis.yml` how they are called to run them manually.

A few major libraries we depend on:

- [SQLAlchemy](https://www.sqlalchemy.org) for ORM (object-relational mapping)
- [Flask Application Builder (FAB)](https://flask-appbuilder.readthedocs.io) for the UI
- [Alembic](https://alembic.sqlalchemy.org) for database migrations

## Setting up a development environment

There are three ways to install an Apache Airflow development environment:

1. Using tools and libraries installed natively on your system
1. Using a single Docker container
1. Using Docker Compose and Airflow's CI scripts

### 1. Using tools and libraries installed natively on your system
Install Python (>=3.5.*), MySQL, and libxml by using system-level package managers like yum, apt-get for Linux, or Homebrew for Mac OS at first. Refer to the base CI Dockerfile for a comprehensive list of required packages.

Then install Python development requirements. It is usually best to work in a virtualenv:

```bash
cd $AIRFLOW_HOME
virtualenv env
source env/bin/activate
pip install -e '.[devel]'
```

### 2. Using a single Docker container
Docker isolates everything from the rest of your system which you might prefer to avoid mixing with other development environments.

```bash
# Start docker in your Airflow directory
docker run -t -i -v `pwd`:/airflow/ -w /airflow/ python:3 bash

# To install all of Airflows dependencies to run all tests (this is a lot)
pip install -e .

# To run only certain tests install the devel requirements and whatever is required
# for your test.  See setup.py for the possible requirements. For example:
pip install -e '.[gcp,devel]'

# Init the database
airflow initdb

nosetests -v tests/hooks/test_druid_hook.py

  test_get_first_record (tests.hooks.test_druid_hook.TestDruidDbApiHook) ... ok
  test_get_records (tests.hooks.test_druid_hook.TestDruidDbApiHook) ... ok
  test_get_uri (tests.hooks.test_druid_hook.TestDruidDbApiHook) ... ok
  test_get_conn_url (tests.hooks.test_druid_hook.TestDruidHook) ... ok
  test_submit_gone_wrong (tests.hooks.test_druid_hook.TestDruidHook) ... ok
  test_submit_ok (tests.hooks.test_druid_hook.TestDruidHook) ... ok
  test_submit_timeout (tests.hooks.test_druid_hook.TestDruidHook) ... ok
  test_submit_unknown_response (tests.hooks.test_druid_hook.TestDruidHook) ... ok

  ----------------------------------------------------------------------
  Ran 8 tests in 3.036s

  OK
```

The Airflow code is mounted inside of the Docker container and installed "editable" (with `-e`), so if you change any code, it is directly available in the environment in your container.

### 3. Using Docker Compose and Airflow's CI scripts

If you prefer a bit more full-fledged development environment, where you can test e.g. against a Postgres database, you will need multiple Docker containers. Docker Compose is a convenient tool to manage multiple containers. Start a Docker container with Compose for development to avoid installing the packages directly on your system. The following will give you a shell inside a container, run all required service containers (MySQL, PostgresSQL, krb5 and so on) and install all the dependencies:

```bash
docker-compose -f scripts/ci/docker-compose.yml run airflow-testing bash
# From the container
export TOX_ENV=py35-backend_mysql-env_docker
/app/scripts/ci/run-ci.sh
```

If you wish to run individual tests inside of Docker environment you can do as follows:

```bash
# From the container (with your desired environment) with druid hook
export TOX_ENV=py35-backend_mysql-env_docker
/app/scripts/ci/run-ci.sh -- tests/hooks/test_druid_hook.py
```

## Building front-end assets

### Setting up the Node/npm JavaScript environment

`airflow/www/` contains all npm-managed, front end assets. Flask-Appbuilder itself comes bundled with jQuery 
and bootstrap. While these may be phased out over time, these packages are currently not managed with npm.

### Node/npm versions

Make sure you are using recent versions of node and npm. No problems have been found with node>=8.11.3 and 
npm>=6.1.3.

### Using npm to generate bundled files

#### npm

First, npm must be available in your environment. If it is not you can run the following commands (taken from 
[this source](https://gist.github.com/DanHerbert/9520689)):

```bash
brew install node --without-npm
echo prefix=~/.npm-packages >> ~/.npmrc
curl -L https://www.npmjs.com/install.sh | sh
```

The final step is to add `~/.npm-packages/bin` to your `PATH` so commands you install globally are usable. Add
something like this to your `.bashrc` file, then `source ~/.bashrc` to reflect the change:

```bash
export PATH="$HOME/.npm-packages/bin:$PATH"
```

#### npm packages

To install third party libraries defined in `package.json`, run the following within the `airflow/www/` 
directory which will install them in a new `node_modules/` folder within `www/`:

```bash
# from the root of the repository, move to where our JS package.json lives
cd airflow/www/
# run npm install to fetch all the dependencies
npm install
```

To parse and generate bundled files for airflow, run either of the following commands. The `dev` flag will 
keep the npm script running and re-run it upon any changes within the assets directory.

```bash
# Compiles the production / optimized js & css
npm run prod

# Start a web server that manages and updates your assets as you modify them
npm run dev
```

#### Upgrading npm packages

Should you add or upgrade a npm package, which involves changing `package.json`, you'll need to re-run 
`npm install` and push the newly generated `package-lock.json` file so we get the reproducible build.

#### JavaScript style guide

We try to enforce a more consistent style and try to follow the JS community guidelines. Once you add or 
modify any JavaScript code in the project, please make sure it follows the guidelines defined in 
[Airbnb JavaScript Style Guide](https://github.com/airbnb/javascript). Apache Airflow uses 
[ESLint](https://eslint.org/) as a tool for identifying and reporting on patterns in JavaScript, which can be 
used by running any of the following commands:

```bash
# Check JS code in .js and .html files, and report any errors/warnings
npm run lint

# Check JS code in .js and .html files, report any errors/warnings and fix them if possible
npm run lint:fix
```

# Developing Airflow

## Running unit tests

To run tests locally, once your unit test environment is set up you should be able to run 
``./run_unit_tests.sh`` at will. For example, in order to just execute the "core" unit tests, run the 
following:

```bash
./run_unit_tests.sh tests.core:CoreTest -s --logging-level=DEBUG
```

Or a single test:
```bash
./run_unit_tests.sh tests.core:CoreTest.test_check_operators -s --logging-level=DEBUG
```

Another example:
```bash
./run_unit_tests.sh tests.contrib.operators.test_dataproc_operator:DataprocClusterCreateOperatorTest.test_create_cluster_deletes_error_cluster  -s --logging-level=DEBUG
```

To run the whole test suite with Docker Compose, do:
```bash
# Install Docker Compose first, then this will run the tests
docker-compose -f scripts/ci/docker-compose.yml run airflow-testing /app/scripts/ci/run-ci.sh
```

## Setting up your own Travis CI

Every PR made to Airflow is automatically passed through the Airflow CI pipeline, which runs on Travis. If you
want to run the same pipeline without having to submit a PR, you can set up your own Travis CI.

Register your Airflow fork in Travis ([guide](https://docs.travis-ci.com/user/tutorial)) and the pipeline will
run on every push. 

Note that travis-ci.org is legacy; make sure you register your fork on
[travis-ci.com](https://travis-ci.com).

## Writing documentation

The latest API documentation is usually available [here](https://airflow.apache.org/). To generate a local 
version, install the `doc` extra in your development environment.

```bash
pip install -e '.[doc]'
```

Generate and serve the documentation by running:

```bash
cd docs
./build.sh
./start_doc_server.sh
```

Only a subset of the API reference documentation builds. Install additional extras to build the full API
reference.

## Changing the metastore schema

When developing features the need may arise to persist information to the metadata database. Airflow uses 
[Alembic](https://bitbucket.org/zzzeek/alembic) to handle schema changes. A history of all schema migrations 
is stored in Python scripts in `airflow/migrations/versions/`. When creating the metastore, or upgrading to a new
Airflow version, these scripts are executed. To generate a new schema change:

```
# Starting at the root of the project
$ cd airflow
$ alembic revision -m "add new column to table"
  Generating .../airflow/migrations/versions/2da817564f87_add_new_column_to_table.py ... done
```

This generates a file in `airflow/migrations/versions/` which contains the changes to make to the schema. In 
that file, implement the changes in the `upgrade()` and `downgrade()` functions for respectively upgrading and 
downgrading the schema.

You can inspect the entire history of schema changes with `alembic history`:

```
$ alembic history
021ae05a82b6 -> 2da817564f87 (head), add new column to table
6e96a59344a4, 004c1210f153 -> 021ae05a82b6 (mergepoint), empty message
939bb1e647c8 -> 6e96a59344a4, Make TaskInstance.pool not nullable
939bb1e647c8 -> 004c1210f153, increase queue name size limit
...
```

## Conventions

- We apply a line length of 110 characters. For IntelliJ/PyCharm, you can configure to display a vertical line
at 110 characters in Preferences -> Editor -> Code Style -> Hard Wrap at -> set to 110.
- We apply the reStructuredText docstring format.

# Pull request guidelines

Before you submit a pull request from your forked repo, check that it meets these guidelines:

- Please make sure a PR touches only a single feature. So e.g. when adding a new argument to an operator, add 
only that one argument and don't fix a typo in a different argument. Unrelated changes are confusing to a 
reviewer and make it harder to trace changes for others and your future self. We rather have 2 PRs touching 2 
features, than a single PR changing multiple features.
- Every PR, except for documentation changes, requires an associated JIRA issue.
- Every commit subject, except for documentation changes, must start with "[AIRFLOW-1234]" where 1234 references the related JIRA issue.
- Documentation only changes do not require a JIRA issue and the commit subject can start with "[AIRFLOW-XXX]".
- The PR title must also start with [AIRFLOW-XXX] or [AIRFLOW-1234].
- Every PR should consist of a single commit. Please [rebase your fork](http://stackoverflow.com/a/7244456/1110993) and squash commits.
- Please read this excellent [article](http://chris.beams.io/posts/git-commit/) on commit messages and adhere to them. It makes the lives of those who come after you a lot easier. 

# Types of contributions

We distinguish a few contribution categories. In general, for all categories, keep the scope for any contribution as narrow as possible to make it easier to implement, review and trace for others and your future self.

## Documentation

Documentation changes are always greatly appreciated and are good beginner issues. They don't require a 
related JIRA issue. Both the commit subject and PR title can start with "[AIRFLOW-XXX]".

## Bug fixes & new small features

Most changes fall in this category and add/alter some Python code, e.g. a new operator. This requires a JIRA 
ticket, even if the change is small. Please refer to the JIRA issue number in your commit subject and PR 
title.

## Large changes

For large changes we require an [Airflow Improvement Proposal (AIP)](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvements+Proposals), where the envisioned change is described, discussed and voted on by the community. The distinction between a small and large change is a grey area and is up to yourself. 

## Breaking changes & deprecation warnings

Breaking changes are only introduced in new major versions. If altering existing functionality, please add a deprecated warning first to inform users of the upcoming change. Also update UPDATING.md to inform users about the change.

## Other feedback

Other feedback about the project can be given on various channels:

- [JIRA](https://issues.apache.org/jira/browse/AIRFLOW)
- [Slack](https://apache-airflow-slack.herokuapp.com)
- [Mailing list](https://lists.apache.org/list.html?dev@airflow.apache.org)

Please remember that this is a volunteer-driven project, and most work is done in spare time :-)

# Tool specific tips & tricks

The section provides some pointers for using various tools in the Airflow project.

## reStructuredText

reStructuredText is used for documentation. A few pointers:

- Always document your methods/functions. Somebody else does not have the same context when reading your code 
and documentation helps understanding the functionality.
- Triple quotes to start PyDoc under a function signature:

  ```python
  def dosomething():
      """This function does something..."""
  ```

- Convention is to write the triple quotes + text if it's a one-liner and fits within 110 characters, 
otherwise write on a newline:

  ```python
  def dosomething():
      """Within 110 chars"""
  
  def longfunc():
      """
      ... a >110 char description ...
      """
  
  def short_with_args(arg):
      """
      Short but with arg description.
    
      :param arg: argument description
      """
  ```

- Note that param types can be listed both separate (`param` & `type` on separate lines) and on a single line:

  ```python
  def one_liner_param(arg):
      """
      Do something.
    
      :param str arg: argument description
      """
  
  def two_liner_param(arg):
      """
      Do something.
    
      :param arg: argument description
      :type arg: str
      """
  ```

## Pylint

You can disable specific messages if desired. This can be done in various ways.

To disable a block of code (typically used if the message is raised multiple consecutive times:

```python
# pylint: disable=line-too-long
print("very long line...")
print("another long line...")
print("yet another long line...")
# pylint: enable=line-too-long
```

If `# pylint: disable` is placed on a separate line, everything after that line is disabled and can (not 
required) be enabled again by `# pylint: enable` on another line.

If you wish to disable only a single line, you can also do:

```python
print("very long line...")  # pylint: disable=line-too-long
```

This only disables the message for that specific line and does not require to explicitly enable the message 
again. In-line disables work in any place, e.g. if the message complains about too many arguments in a 
function, place it at the function definition line:

```python
def foobar(many, many2, args):  # pylint: disable=too-many-arguments
    # ....
```

To disable multiple messages simultaneously:

```python
# pylint: disable=too-many-arguments,line-too-long
```

Generally the one-liner is preferred because you don't have to enable the message again. 
