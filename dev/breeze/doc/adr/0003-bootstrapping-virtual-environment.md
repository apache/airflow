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

- [3. Bootstrapping the virtual environment](#3-bootstrapping-the-virtual-environment)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Alternatives considered](#alternatives-considered)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 3. Bootstrapping the virtual environment

Date: 2021-12-06

## Status

Superseded by [10. Use pipx to install breeze](0010-use-pipx-to-install-breeze.md)

## Context

Since Breeze is written in Python, it needs to be run in its own virtual environment.
This virtual environment is different from Airflow virtualenv as it contains only a
small set of tools (for example rich) that are not present in the standard Python
library. We want to keep the virtualenv separated, because setting up Airflow
virtualenv is hard (especially if you consider cross-platform use). The virtualenv
is needed mainly to run the script that will actually manage airflow installation
and dependencies, in the form of Docker images which are part of Breeze.

This virtualenv needs to be easy to setup and it should support the "live" nature
of Breeze. The idea is that the user of Breeze does not have to do any action
to update to the latest version of the virtualenv, when new dependencies are
added, also when new Breeze functionalities are added, they should be automatically
available for the user after the repository is updated to latest version.

User should not have to think about installing and upgrading Breeze separately from
switching to different Airflow tag or branch - moreover, the Breeze environment
should automatically adapt to the version and Branch the user checked out. By its
nature Airflow Breeze (at least for quite a while) will be evolving together with
Airflow and it will live in the same repository and new features and behaviours
will be added continuously.

The workflow that needs to be supported should tap into the regular workflow
of the user who is developing Airflow.

* git checkout branch

./Breeze should use the version of Breeze that is available in this version

* git rebase --onto apache/main

./Breeze should be automatically updated to the latest version available
in main (including dependencies)

Also if someone develops Breeze itself, the experience should be seamlessly
integrated - modification of Breeze code locally should be automatically
reflected in the Breeze environment of the user who is modifying Breeze.

The user should not have to re-install/update Breeze to automatically use
the modifying Breeze source code when running Breeze commands and testing
then with Airflow.

Breeze is also used as part of CI - common Python functions and libraries
are used across both Breeze development environment and Continuous
Integration we run. It's been established practice of the CI is that the logic
of the CI is stored in the same repository as the source code of the
application it tests and part of the Breeze functions are shared with CI.

In the future when breeze stabilizes and its update cadence will be
much slower (which is likele as it happened with the Breeze predecessor)
there could be an option that Breeze is installed as separate package and
same released Breeze version could be ued to manage multiple Airflow
versions, for that we might want to release Breeze as a separate package
in PyPI. However since there is the CI integration, the source code
version of Breeze will remain as part of the Airflow's source code.


## Decision

The decision is to implement Breeze in a subfolder (`dev/breeze2/`) of
Apache Airflow as a Python project following the standard setuptools
enabled project. The project contains setup.py and dependencies described
in pyproject.toml and contains both source code and tests for Breeze code.

The sub-project could be used in the future to produce a  PyPI package
(we reserved such package in PyPI), however its main purpose is
to install Breeze in a separate virtualenv bootstrapped
automatically in editable mode.

There are two ways you will be able to install `breeze` - locally in
repository using ./breeze bootstrapping script and using `pipx`.

The bootstrapping Python script (`breeze` in the main repository
of Airflow) performs the following tasks:

* when run for the first time it creates `.build/breeze2/venv` virtual
  environment (Python3.6+ based) - with locally installed `dev`
  project in editable mode (`pip install -e .`) - this makes sure
  that the users of Breeze will use the latest version of Breeze
  available in their version of the repository
* when run subsequently, it will check if setup files changed for
  Breeze (dependencies changed) and if they did it will automatically
  reinstall the environment, adding missing dependencies
* after managing the venv, the breeze script will simply execute
  the actual breeze script in the `.build/venv` passing the
  parameters to the script. For the user, the effect will be same
  as activating the virtualenv and executing the ./breeze from
  there (but it will happen automatically and invisibly for the
  user
* In Windows environment where you have no easy/popular mechanism
  of running scripts with shebang (#!) equivalent in Posix
  environments, Users will be able to locally build (using
  `pyinstaller` a `breeze.exe` frozen Python script that will
  essentially do the same, they could also use `python breeze`
  command or switch to Git Bash to utilize the shebang feature
  (Git Bash comes together with Git when installed on Windows)
* The second option is to use `pipx` to install breeze.
  The `pipx` is almost equivalent to what the Bootstrapping does
  and many users might actually choose to install Breeze this
  way - and we will add it as an option to install Breeze
  with pipx `pipx install -e <BREEZE FOLDER>` provides the right
  installation instruction. The installation can be updated
  by `pipx install --force -e <BREEZE FOLDER>`.
  The benefit of using `pipx` is that Breeze becomes
  available on the path when you install it this way, also
  it provides out-of-the box Windows support. The drawback is
  that when new dependencies are added, they will not be
  automatically installed and that you need to manually force
  re-installation if new dependencies are used - which is not
  as seamlessly integrate in the regular development
  environment, and it might create some confusions for the
  users who would have to learn `pipx` and its commands.
  Another drawback of `pipx` is that installs one global
  version of breeze for all projects, where it is quite
  possible that someone has two different versions of
  Airflow repository checked out and the bootstrapping
  script provides this capability.

The bootstrapping script is temporary measure, until the
dependencies of Breeze stabilize enough that the need
to recreate the virtual environment by `pipx` will be
very infrequent. In this case `pipx` provides better
user experience, and we might decide even to remove the
bootstrapping script and switch fully to `pipx`

## Alternatives considered

The alternatives considered were:

* `nox` - this is a tool to manage virtualenv for testing, while
  it has some built in virtualenv capabilities, it is an
  additional tool that needs to be installed and it lacks
  the automation of checking and recreation of the virtualenv
  when needed (you need to manually run nox to update environment)
  Alsoi it is targeted for building multiple virtualenv
  for tests - it has nice pytest integration for example, but it
  lacks support for managing editable installs for a long time.

* `pyenv` - this is the de-facto standard for maintenance of
  virtualenvs. it has the capability of creation and switching
  between virtualenvs easily. Together with some of its plugins
  (pyenv-virtualenv and auto-activation) it could serve the
  purpose quite well. However the problem is that if you
  also use `pyenv` to manage your `airflow` virtualenv this might
  be source of confusion. Should I activate airflow virtualenv
  or breeze venv to run tests? Part of Breeze experience is
  to activate local Airflow virtualenv for IDE integration and
  since this is different than simple Breeze virtualenv, using
  pytest and autoactivation in this case might lead to a lot
  of confusion. Keeping the Breeze virtualenv "hidden" and
  mostly "used" but not deliberately activated is a better
  choice - especially that most users will simply "use" breeze
  as an app rather than activate the environment deliberately.
  Also choosing `pyenv` and its virtualenv plugin would
  add extra, unnecessary steps and prerequisites for Breeze.


## Consequences

Using Breeze for new users will be much simpler, without
having to install any prerequisites. The virtualenv used by
breeze will be hidden from the user, and used behind the
scenes - and the dependencies used will be automatically
installed when needed. This will allow to seamlessly
integrate Breeze tool in the develiopment experience without
having to worry about extra maintenance needed.
