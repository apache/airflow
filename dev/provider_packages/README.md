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

- [Backport packages](#backport-packages)
- [What the backport packages are](#what-the-backport-packages-are)
- [Content of the release notes](#content-of-the-release-notes)
  - [Generating release notes](#generating-release-notes)
  - [Preparing backport packages](#preparing-backport-packages)
- [Testing provider package scripts](#testing-provider-package-scripts)
  - [Backport packages](#backport-packages-1)
  - [Regular packages](#regular-packages)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Backport packages

# What the backport packages are

The Backport Provider packages are packages (per provider) that make it possible to easily use Hooks,
Operators, Sensors, and Secrets from the 2.0 version of Airflow in the 1.10.* series.

The release manager prepares backport packages separately from the main Airflow Release, using
`breeze` commands and accompanying scripts. This document provides an overview of the command line tools
needed to prepare backport packages.

# Content of the release notes

Each of the backport packages contains Release notes in the form of the README.md file that is
automatically generated from history of the changes and code of the provider.

The script generates all the necessary information:

* summary of requirements for each backport package
* list of dependencies (including extras to install them) when package
  depends on other providers packages
* table of new hooks/operators/sensors/protocols/secrets
* table of moved hooks/operators/sensors/protocols/secrets with the
  information where they were moved from
* changelog of all the changes to the provider package. This will be
  automatically updated with an incremental changelog whenever we decide to
  release separate packages.

The script generates two types of files:

* BACKPORT_PROVIDERS_CHANGES_YYYY.MM.DD.md which keeps information about changes (commits) in a particular
  version of the provider package. The file for latest release gets updated when you iterate with
  the same new date/version, but it never changes automatically for already released packages.
  This way - just before the final release, you can manually correct the changes file if you
  want to remove some changes from the file.

* README.md which is regenerated every time you run the script (unless there are no changes since
  the last time you generated the release notes

Note that our CI system builds the release notes for backport packages automatically with every build and
current date - this way you might be sure the automated generation of the release notes continues to
work. You can also preview the generated readme files (by downloading artifacts from GitHub Actions).
The script does not modify the README and CHANGES files if there is no change in the repo for that provider.


## Generating release notes

When you want to prepare release notes for a package, you need to run:

```
./breeze prepare-provider-readme [YYYY.MM.DD] <PACKAGE_ID> ...
```


* YYYY.MM.DD - is the CALVER version of the package to prepare. Note that this date cannot be earlier
  than the already released version (the script will fail if it will be). It can be set in the future
  anticipating the future release date. If you do not specify date, the date will be taken from the last
  generated readme - the last generated CHANGES file will be updated.

* <PACKAGE_ID> is usually directory in the `airflow/providers` folder (for example `google` but in several
  cases, it might be one level deeper separated with `.` for example `apache.hive`

You can run the script with multiple package names if you want to prepare several packages at the same time.
Before you specify a new version, the last released version is update in case you have any bug fixes
merged in the master recently, they will be automatically taken into account.

Typically, the first time you run release before release, you run it with target release.date:

```
./breeze prepare-provider-readme 2020.05.20 google
```

Then while you iterate with merges and release candidates you update the release date without providing
the date (to update the existing release notes)

```
./breeze prepare-provider-readme google
```


Whenever you are satisfied with the release notes generated you can commit generated changes/new files
to the repository.

## Preparing backport packages

As part of preparation to Airflow 2.0 we decided to prepare backport of providers package that will be
possible to install in the Airflow 1.10.*, Python 3.6+ environment.
Some of those packages will be soon (after testing) officially released via PyPi, but you can build and
prepare such packages on your own easily.

You build those packages in the breeze environment, so you do not have to worry about common environment.

Note that readme release notes have to be generated first, so that the package preparation script reads
the latest version from the latest version of release notes prepared.

* The provider package ids PACKAGE_ID are subdirectories in the ``providers`` directory. Sometimes they
are one level deeper (`apache/hive` folder for example, in which case PACKAGE_ID uses "." to separate
the folders (for example Apache Hive's PACKAGE_ID is `apache.hive` ). You can see the list of all available
providers by running:

```bash
./breeze prepare-provider-packages -- --help
```

The examples below show how you can build selected packages, but you can also build all packages by
omitting the package ids altogether.

* To build the release candidate packages for SVN Apache upload run the following command:

```bash
./breeze prepare-provider-packages --version-suffix-for-svn=rc1 [PACKAGE_ID] ...
```

for example:

```bash
./breeze prepare-provider-packages --version-suffix-for-svn=rc1 http ...
```

* To build the release candidate packages for PyPI upload run the following command:

```bash
./breeze prepare-provider-packages --version-suffix-for-pypi=rc1 [PACKAGE_ID] ...
```

for example:

```bash
./breeze prepare-provider-packages --version-suffix-for-pypi=rc1 http ...
```


* To build the final release packages run the following command:

```bash
./breeze prepare-provider-packages [PACKAGE_ID] ...
```

for example:

```bash
./breeze prepare-provider-packages http ...
```

* For each package, this creates a wheel package and source distribution package in your `dist` folder with
  names following the patterns:

  * `apache_airflow_backport_providers_<PROVIDER>_YYYY.[M]M.[D]D[suffix]-py3-none-any.whl`
  * `apache-airflow-backport-providers-<PROVIDER>-YYYY.[M]M.[D]D[suffix].tar.gz`

Note! Even if we always use the two-digit month and day when generating the readme files,
the version in PyPI does not contain the leading 0s in version name - therefore the artifacts generated
also do not container the leading 0s.

* You can install the .whl packages with `pip install <PACKAGE_FILE>`


# Testing provider package scripts

The backport packages importing and tests execute within the "CI" environment of Airflow -the
same image that is used by Breeze. They however require special mounts (no
sources of Airflow mounted to it) and possibility to install all extras and packages in order to test
importability of all the packages. It is rather simple but requires some semi-automated process:

## Backport packages

1. Prepare backport packages


```shell script
./breeze --backports prepare-provider-packages
```

This prepares all backport packages in the "dist" folder

2. Enter the container:

```shell script
export INSTALL_AIRFLOW_VERSION=1.10.12
export BACKPORT_PACKAGES="true"

./dev/provider_packages/enter_breeze_provider_package_tests.sh
```

(the rest of it is in the container)

3. \[IN CONTAINER\] Install all remaining dependencies and reinstall airflow 1.10:

```shell script
cd /airflow_sources

pip install ".[all]"

pip install "apache-airflow==${INSTALL_AIRFLOW_VERSION}"

cd
```

4. \[IN CONTAINER\] Install the provider packages from /dist

```shell script
pip install /dist/apache_airflow_backport_providers_*.whl
```

5.  \[IN CONTAINER\] Check the installation folder for providers:

```shell script
python3 <<EOF 2>/dev/null
import airflow.providers;
path=airflow.providers.__path__
for p in path._path:
    print(p)
EOF
```

6.  \[IN CONTAINER\] Check if all the providers can be imported
python3 /opt/airflow/dev/import_all_classes.py --path <PATH_REPORTED_IN_THE_PREVIOUS_STEP>


## Regular packages

1. Prepare regular packages

```shell script
./breeze prepare-provider-packages
```

This prepares all backport packages in the "dist" folder

2. Prepare airflow package from sources

```shell script
python setup.py compile_assets sdist bdist_wheel
rm -rf -- *egg-info*
```

This prepares airflow package in the "dist" folder

2. Enter the container:

```shell script
export INSTALL_AIRFLOW_VERSION="wheel"
unset BACKPORT_PACKAGES

./dev/provider_packages/enter_breeze_provider_package_tests.sh
```

(the rest of it is in the container)

3. \[IN CONTAINER\] Install apache-beam.

```shell script
pip install apache-beam[gcp]
```

4. \[IN CONTAINER\] Install the provider packages from /dist

```shell script
pip install --no-deps /dist/apache_airflow_providers_*.whl
```

Note! No-deps is because we are installing the version installed from wheel package.

5.  \[IN CONTAINER\] Check the installation folder for providers:

```shell script
python3 <<EOF 2>/dev/null
import airflow.providers;
path=airflow.providers.__path__
for p in path._path:
    print(p)
EOF
```

6.  \[IN CONTAINER\] Check if all the providers can be imported
python3 /opt/airflow/dev/import_all_classes.py --path <PATH_REPORTED_IN_THE_PREVIOUS_STEP>
