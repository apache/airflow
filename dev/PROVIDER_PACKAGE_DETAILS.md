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
- [Overview](#overview)

- [Overview](#overview)
- [Deciding when to release](#deciding-when-to-release)
  - [Backport Packages](#backport-packages)
  - [Regular Provider packages](#regular-provider-packages)
- [Generating release notes](#generating-release-notes)
  - [Backport providers](#backport-providers)
  - [Regular providers](#regular-providers)
- [Content of the release notes](#content-of-the-release-notes)
- [Preparing packages](#preparing-packages)
  - [Backport provider packages](#backport-provider-packages)
  - [Regular provider packages](#regular-provider-packages)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


# Overview

This document describes the process of preparing provider packages for release and releasing them.
The provider packages are packages (per provider) that are not part of the core Airflow.

They are prepared in two variants:

* Backport Provider Packages - those are the packages that can be installed in Airflow 1.10 line.
  They provide an easy migration path to Airflow 2.0 for anyone that still uses Airflow 1.10.

* Regular Provider Packages - those are the packages that can be installed in Airflow 2.0. Basic
  Airflow release contains just core packages and operators. All the remaining providers have
  to be installed separately. When you install an extra, the right provider package should be installed
  automatically. Regular Provider Packages are Work In Progress and some details and processes are going
  to be hashed-out during Alpha and Beta releases of Airflow 2.0.

Once you release the packages, you can simply install them with:

```
pip install apache-airflow-backport-providers-<PROVIDER>[<EXTRAS>]
```

for backport provider packages, or

```
pip install apache-airflow-providers-<PROVIDER>[<EXTRAS>]
```

for regular provider packages.

Where `<PROVIDER>` is the provider id and `<EXTRAS>` are optional extra packages to install.
You can find the provider packages dependencies and extras in the README.md files in each provider
package (in `airflow/providers/<PROVIDER>` folder) as well as in the PyPI installation page.

Backport providers are a great way to migrate your DAGs to Airflow-2.0 compatible DAGs. You can
switch to the new Airflow-2.0 packages in your DAGs, long before you attempt to migrate
airflow to 2.0 line.

# Deciding when to release

## Backport Packages

You can release backport packages separately on an ad-hoc basis, whenever we find that a given provider needs
to be released - due to new features or due to bug fixes. You can release each backport package
separately - although we decided to release all backport packages together in one go 2020.05.10.

We are using the [CALVER](https://calver.org/) versioning scheme for the backport packages. We also have an
automated way to prepare and build the packages, so it should be very easy to release the packages often and
separately.

## Regular Provider packages

Each provider package has its own version maintained separately when contributors implement changes,
marking those as patches/features/backwards incompatible changes.

Details to be hashed out in [the related issue](https://github.com/apache/airflow/issues/11425)


# Generating release notes

## Backport providers

When you want to prepare release notes for a package, you need to run:

```
./breeze --backports prepare-provider-readme [YYYY.MM.DD] <PACKAGE_ID> ...
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
./breeze --backports prepare-provider-readme 2020.05.20 google
```

Then while you iterate with merges and release candidates you update the release date without providing
the date (to update the existing release notes)

```
./breeze --backports prepare-provider-readme google
```


Whenever you are satisfied with the release notes generated you can commit generated changes/new files
to the repository.


## Regular providers

When you want to prepare release notes for a package, you need to run:

```
./breeze --backports prepare-provider-readme <PACKAGE_ID> ...
```

The version for each package is going to be updated separately for each package when we agree to the
process.

Details to be hashed out in [the related issue](https://github.com/apache/airflow/issues/11425)

* <PACKAGE_ID> is usually directory in the `airflow/providers` folder (for example `google` but in several
  cases, it might be one level deeper separated with `.` for example `apache.hive`

You can run the script with multiple package names if you want to prepare several packages at the same time.

If you do not change version number, you can iterate with merges and release candidates you update the
release date without providing
the date (to update the existing release notes)

```
./breeze --backports prepare-provider-readme google
```


Whenever you are satisfied with the release notes generated you can commit generated changes/new files
to the repository.


# Content of the release notes

The script generates all the necessary information:

* summary of requirements for each backport package
* list of dependencies (including extras to install them) when package
  depends on other providers packages
* table of new hooks/operators/sensors/protocols/secrets
* table of moved hooks/operators/sensors/protocols/secrets with the
  information where they were moved from
* changelog of all the changes to the provider package (this will be
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

# Preparing packages

## Backport provider packages

As part of preparation to Airflow 2.0 we decided to prepare backport of providers package that will be
possible to install in the Airflow 1.10.*, Python 3.6+ environment.

You can build those packages in the breeze environment, so you do not have to worry about common environment.

Note that readme release notes have to be generated first, so that the package preparation script reads
the latest version from the latest version of release notes prepared.

* The provider package ids PACKAGE_ID are subdirectories in the ``providers`` directory. Sometimes they
are one level deeper (`apache/hive` folder for example, in which case PACKAGE_ID uses "." to separate
the folders (for example Apache Hive's PACKAGE_ID is `apache.hive` ). You can see the list of all available
providers by running:

```bash
./breeze --backports prepare-provider-packages -- --help
```

The examples below show how you can build selected packages, but you can also build all packages by
omitting the package ids altogether.

* To build the release candidate packages for SVN Apache upload run the following command:

```bash
./breeze --backports prepare-provider-packages package-format both --version-suffix-for-svn=rc1 [PACKAGE_ID] ...
```

for example:

```bash
./breeze --backports prepare-provider-packages package-format both --version-suffix-for-svn=rc1 http ...
```

* To build the release candidate packages for PyPI upload run the following command:

```bash
./breeze --backports prepare-provider-packages package-format both --version-suffix-for-pypi=rc1 [PACKAGE_ID] ...
```

for example:

```bash
./breeze --backports prepare-provider-packages package-format both --version-suffix-for-pypi=rc1 http ...
```


* To build the final release packages run the following command:

```bash
./breeze --backports prepare-provider-packages package-format both [PACKAGE_ID] ...
```

for example:

```bash
./breeze --backports prepare-provider-packages package-format both  http ...
```

* For each package, this creates a wheel package and source distribution package in your `dist` folder with
  names following the patterns:

  * `apache_airflow_backport_providers_<PROVIDER>_YYYY.[M]M.[D]D[suffix]-py3-none-any.whl`
  * `apache-airflow-backport-providers-<PROVIDER>-YYYY.[M]M.[D]D[suffix].tar.gz`

Note! Even if we always use the two-digit month and day when generating the readme files,
the version in PyPI does not contain the leading 0s in version name - therefore the artifacts generated
also do not container the leading 0s.

* You can install the .whl packages with `pip install <PACKAGE_FILE>`

## Regular provider packages

Airflow 2.0 is released as separate core package and separate set of provider packages.

You can build those packages in the breeze environment, so you do not have to worry about common environment.

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
./breeze prepare-provider-packages package-format both --version-suffix-for-svn=rc1 [PACKAGE_ID] ...
```

for example:

```bash
./breeze prepare-provider-packages package-format both --version-suffix-for-svn=rc1 http ...
```

* To build the release candidate packages for PyPI upload run the following command:

```bash
./breeze prepare-provider-packages package-format both --version-suffix-for-pypi=rc1 [PACKAGE_ID] ...
```

for example:

```bash
./breeze prepare-provider-packages package-format both --version-suffix-for-pypi=rc1 http ...
```


* To build the final release packages run the following command:

```bash
./breeze prepare-provider-packages package-format both [PACKAGE_ID] ...
```

for example:

```bash
./breeze prepare-provider-packages package-format both http ...
```

* For each package, this creates a wheel package and source distribution package in your `dist` folder with
  names following the patterns:

  * `apache_airflow_providers_<PROVIDER>_MAJOR.MINOR.PATCHLEVEL[suffix]-py3-none-any.whl`
  * `apache-airflow-providers-<PROVIDER>-MAJOR.MINOR.PATCHLEVEL[suffix].tar.gz`

Where ``MAJOR.MINOR.PATCHLEVEL`` is the semver version of the packages.

* You can install the .whl packages with `pip install <PACKAGE_FILE>`

Releasing the packages is described in [README.md](README.md)
