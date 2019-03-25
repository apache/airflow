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

# Table of Contents
  * [TOC](#table-of-contents)
  * [Types of Contributions](#types-of-contributions)
      - [Report Bugs](#report-bugs)
      - [Fix Bugs](#fix-bugs)
      - [Implement Features](#implement-features)
      - [Improve Documentation](#improve-documentation)
      - [Submit Feedback](#submit-feedback)
  * [Documentation](#documentation)
  * [Development and Testing](#development-and-testing)
      - [Setting up a development environment](#setting-up-a-development-environment)
      - [Running unit tests](#running-unit-tests)
  * [Pull requests guidelines](#pull-request-guidelines)
  * [Changing the Metadata Database](#changing-the-metadata-database)

## Types of Contributions

### Report Bugs

Report bugs through [Apache Jira](https://issues.apache.org/jira/browse/AIRFLOW)

Please report relevant information and preferably code that exhibits
the problem.

### Fix Bugs

Look through the Jira issues for bugs. Anything is open to whoever wants
to implement it.

### Implement Features

Look through the [Apache Jira](https://issues.apache.org/jira/browse/AIRFLOW) for features. Any unassigned "Improvement" issue is open to whoever wants to implement it.

We've created the operators, hooks, macros and executors we needed, but we
made sure that this part of Airflow is extensible. New operators,
hooks, macros and executors are very welcomed!

### Improve Documentation

Airflow could always use better documentation,
whether as part of the official Airflow docs,
in docstrings, `docs/*.rst` or even on the web as blog posts or
articles.

### Submit Feedback

The best way to send feedback is to open an issue on [Apache Jira](https://issues.apache.org/jira/browse/AIRFLOW)

If you are proposing a feature:

- Explain in detail how it would work.
- Keep the scope as narrow as possible, to make it easier to implement.
- Remember that this is a volunteer-driven project, and that contributions are welcome :)

## Documentation

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

Only a subset of the API reference documentation builds. Install additional
extras to build the full API reference.

## Development and Testing

### Setting up a development environment

There are three ways to setup an Apache Airflow development environment.

1. Using tools and libraries installed directly on your system

  Install Python (2.7.x or 3.5.x), MySQL, and libxml by using system-level package
  managers like yum, apt-get for Linux, or Homebrew for Mac OS at first. Refer to the [base CI Dockerfile](https://github.com/apache/airflow-ci/blob/master/Dockerfile) for
  a comprehensive list of required packages.

  Then install python development requirements. It is usually best to work in a virtualenv:

  ```bash
  cd $AIRFLOW_HOME
  virtualenv env
  source env/bin/activate
  pip install -e '.[devel]'
  ```

2. Using a Docker container

  Go to your Airflow directory and start a new docker container. You can choose between Python 2 or 3, whatever you prefer.

  ```
  # Start docker in your Airflow directory
  docker run -t -i -v `pwd`:/airflow/ -w /airflow/ python:3 bash

  # Install Airflow with all the required dependencies,
  # including the devel which will provide the development tools
  pip install -e '.[hdfs,hive,druid,devel]'

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

  The Airflow code is mounted inside of the Docker container, so if you change something using your favorite IDE, you can directly test it in the container.

3. Using [Docker Compose](https://docs.docker.com/compose/) and Airflow's CI scripts

  Start a docker container through Compose for development to avoid installing the packages directly on your system. The following will give you a shell inside a container, run all required service containers (MySQL, PostgresSQL, krb5 and so on) and install all the dependencies:

  ```bash
  docker-compose -f scripts/ci/docker-compose.yml run airflow-testing bash
  # From the container
  export TOX_ENV=py27-backend_mysql-env_docker
  /app/scripts/ci/run-ci.sh
  ```

  If you wish to run individual tests inside of Docker environment you can do as follows:

  ```bash
  # From the container (with your desired environment) with druid hook
  export TOX_ENV=py27-backend_mysql-env_docker
  /app/scripts/ci/run-ci.sh -- tests/hooks/test_druid_hook.py
  ```


### Running unit tests

To run tests locally, once your unit test environment is setup (directly on your
system or through our Docker setup) you should be able to simply run
``./run_unit_tests.sh`` at will.

For example, in order to just execute the "core" unit tests, run the following:

```
./run_unit_tests.sh tests.core:CoreTest -s --logging-level=DEBUG
```

or a single test method:

```
./run_unit_tests.sh tests.core:CoreTest.test_check_operators -s --logging-level=DEBUG
```

To run the whole test suite with Docker Compose, do:

```
# Install Docker Compose first, then this will run the tests
docker-compose -f scripts/ci/docker-compose.yml run airflow-testing /app/scripts/ci/run-ci.sh
```

Alternatively, you can also set up [Travis CI](https://travis-ci.org/) on your repo to automate this.
It is free for open source projects.

Another great way of automating linting and testing is to use [Git Hooks](https://git-scm.com/book/uz/v2/Customizing-Git-Git-Hooks). For example you could create a `pre-commit` file based on the Travis CI Pipeline so that before each commit a local pipeline will be triggered and if this pipeline fails (returns an exit code other than `0`) the commit does not come through.
This "in theory" has the advantage that you can not commit any code that fails that again reduces the errors in the Travis CI Pipelines.

Since there are a lot of tests the script would last very long so you probably only should test your new feature locally.

The following example of a `pre-commit` file allows you..
- to lint your code via flake8
- to test your code via nosetests in a docker container based on python 2
- to test your code via nosetests in a docker container based on python 3

```
#!/bin/sh

GREEN='\033[0;32m'
NO_COLOR='\033[0m'

setup_python_env() {
    local venv_path=${1}

    echo -e "${GREEN}Activating python virtual environment ${venv_path}..${NO_COLOR}"
    source ${venv_path}
}
run_linting() {
    local project_dir=$(git rev-parse --show-toplevel)

    echo -e "${GREEN}Running flake8 over directory ${project_dir}..${NO_COLOR}"
    flake8 ${project_dir}
}
run_testing_in_docker() {
    local feature_path=${1}
    local airflow_py2_container=${2}
    local airflow_py3_container=${3}

    echo -e "${GREEN}Running tests in ${feature_path} in airflow python 2 docker container..${NO_COLOR}"
    docker exec -i -w /airflow/ ${airflow_py2_container} nosetests -v ${feature_path}
    echo -e "${GREEN}Running tests in ${feature_path} in airflow python 3 docker container..${NO_COLOR}"
    docker exec -i -w /airflow/ ${airflow_py3_container} nosetests -v ${feature_path}
}

set -e
# NOTE: Before running this make sure you have set the function arguments correctly.
setup_python_env /Users/feluelle/venv/bin/activate
run_linting
run_testing_in_docker tests/contrib/hooks/test_imap_hook.py dazzling_chatterjee quirky_stallman

```

For more information on how to run a subset of the tests, take a look at the
nosetests docs.

See also the list of test classes and methods in `tests/core.py`.

Feel free to customize based on the extras available in [setup.py](./setup.py)

## Pull Request Guidelines

Before you submit a pull request from your forked repo, check that it
meets these guidelines:

1. The pull request should include tests, either as doctests, unit tests, or both. The airflow repo uses [Travis CI](https://travis-ci.org/apache/airflow) to run the tests and [codecov](https://codecov.io/gh/apache/airflow) to track coverage. You can set up both for free on your fork (see the "Testing on Travis CI" section below). It will help you making sure you do not break the build with your PR and that you help increase coverage.
1. Please [rebase your fork](http://stackoverflow.com/a/7244456/1110993), squash commits, and resolve all conflicts.
1. Every pull request should have an associated [JIRA](https://issues.apache.org/jira/browse/AIRFLOW/?selectedTab=com.atlassian.jira.jira-projects-plugin:summary-panel). The JIRA link should also be contained in the PR description.
1. Preface your commit's subject & PR's title with **[AIRFLOW-XXX]** where *XXX* is the JIRA number. We compose release notes (i.e. for Airflow releases) from all commit titles in a release. By placing the JIRA number in the commit title and hence in the release notes, Airflow users can look into JIRA and GitHub PRs for more details about a particular change.
1. Add an [Apache License](http://www.apache.org/legal/src-headers.html) header to all new files
1. If the pull request adds functionality, the docs should be updated as part of the same PR. Doc string are often sufficient.  Make sure to follow the Sphinx compatible standards.
1. The pull request should work for Python 2.7 and 3.5. If you need help writing code that works in both Python 2 and 3, see the documentation at the [Python-Future project](http://python-future.org) (the future package is an Airflow requirement and should be used where possible).
1. As Airflow grows as a project, we try to enforce a more consistent style and try to follow the Python community guidelines. We currently enforce most [PEP8](https://www.python.org/dev/peps/pep-0008/) and a few other linting rules. It is usually a good idea to lint locally as well using [flake8](https://flake8.readthedocs.org/en/latest/) using `flake8 airflow tests`. `git diff upstream/master -u -- "*.py" | flake8 --diff` will return any changed files in your branch that require linting.
1. Please read this excellent [article](http://chris.beams.io/posts/git-commit/) on commit messages and adhere to them. It makes the lives of those who come after you a lot easier.

### Testing on Travis CI

We currently rely heavily on Travis CI for running the full Airflow test suite
as running all of the tests locally requires significant setup.  You can setup
Travis CI in your fork of Airflow by following the
[Travis CI Getting Started guide][travis-ci-getting-started].

There are two different options available for running Travis CI which are
setup as separate components on GitHub:

1. **Travis CI GitHub App** (new version)
1. **Travis CI GitHub Services** (legacy version)

#### Travis CI GitHub App (new version)

1. Once installed, you can configure the Travis CI GitHub App at
https://github.com/settings/installations -> Configure Travis CI.

1. For the Travis CI GitHub App, you can set repository access to either "All
repositories" for convenience, or "Only select repositories" and choose
`<username>/airflow` in the dropdown.

1. You can access Travis CI for your fork at
`https://travis-ci.com/<username>/airflow`.

#### Travis CI GitHub Services (legacy version)

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

#### Prefer travis-ci.com over travis-ci.org

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


### Changing the Metadata Database

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

## Setting up the node / npm javascript environment

`airflow/www/` contains all npm-managed, front end assets.
Flask-Appbuilder itself comes bundled with jQuery and bootstrap.
While these may be phased out over time, these packages are currently not
managed with npm.

### Node/npm versions

Make sure you are using recent versions of node and npm. No problems have been found with node>=8.11.3 and npm>=6.1.3

### Using npm to generate bundled files

#### npm

First, npm must be available in your environment. If it is not you can run the following commands
(taken from [this source](https://gist.github.com/DanHerbert/9520689))

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

#### npm packages

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

#### Upgrading npm packages

Should you add or upgrade a npm package, which involves changing `package.json`, you'll need to re-run `npm install`
and push the newly generated `package-lock.json` file so we get the reproducible build.

#### Javascript Style Guide

We try to enforce a more consistent style and try to follow the JS community guidelines.
Once you add or modify any javascript code in the project, please make sure it follows the guidelines
defined in [Airbnb JavaScript Style Guide](https://github.com/airbnb/javascript).
Apache Airflow uses [ESLint](https://eslint.org/) as a tool for identifying and reporting on patterns in JavaScript,
which can be used by running any of the following commands.

```bash
# Check JS code in .js and .html files, and report any errors/warnings
npm run lint

# Check JS code in .js and .html files, report any errors/warnings and fix them if possible
npm run lint:fix
```
