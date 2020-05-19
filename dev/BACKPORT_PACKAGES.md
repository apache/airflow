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
- [Backport packages](#backport-packages)

- [Overview](#overview)
- [Deciding when to release](#deciding-when-to-release)
- [Generating release notes](#generating-release-notes)
- [Content of the release notes](#content-of-the-release-notes)
- [Preparing backport packages](#preparing-backport-packages)
- [Releasing the packages](#releasing-the-packages)
  - [Building an RC](#building-an-rc)
  - [Make sure your public key is on id.apache.org and in KEYS](#make-sure-your-public-key-is-on-idapacheorg-and-in-keys)
  - [Publishing to PyPi](#publishing-to-pypi)
  - [Voting on RC candidates](#voting-on-rc-candidates)
  - [Publishing the final release](#publishing-the-final-release)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


# Overview

This document describes the process of preparing backport packages for release and releasing them.
Backport packages are packages (per provider) that make it possible to easily use Hooks, Operators,
Sensors, Protocols, and Secrets from the 2.0 version of Airflow in the 1.10.* series.

Once you release the packages, you can simply install them with:

```
pip install apache-airflow-backport-providers-<PROVIDER>[<EXTRAS>]
```

Where `<PROVIDER>` is the provider id and `<EXTRAS>` are optional extra packages to install.
You can find the provider packages dependencies and extras in the README.md files in each provider
package (in `airflow/providers/<PROVIDER>` folder) as well as in the PyPI installation page.

Backport providers are a great way to migrate your DAGs to Airflow-2.0 compatible DAGs. You can
switch to the new Airflow-2.0 packages in your DAGs, long before you attempt to migrate
airflow to 2.0 line.

# Deciding when to release

You can release backport packages separately on an ad-hoc basis, whenever we find that a given provider needs
to be released - due to new features or due to bug fixes. You can release each backport package
separately - although we decided to release all backport packages together in one go 2020.05.10.

We are using the [CALVER](https://calver.org/) versioning scheme for the backport packages. We also have an
automated way to prepare and build the packages, so it should be very easy to release the packages often and
separately.

# Generating release notes

When you want to prepare release notes for a package, you need to run:

```
./breeze generate-backport-readme -- YYYY.MM.DD <PACKAGE_ID> ...
```


* YYYY.MM.DD - is the CALVER version of the package to prepare. Note that this date cannot be earlier
  than the already released version (the script will fail if it will be). It can be set in the future
  anticipating the future release date. If you do not specify it - current date +3 days will be used.

* <PACKAGE_ID> is usually directory in the `airflow/providers` folder (for example `google` but in several
  cases, it might be one level deeper separated with `.` for example `apache.hive`

You can run the script with multiple package names if you want to prepare several packages at the same time.
You can also re-run the script with the same version (date) - this way - in case you have any bug fixes
merged in the master, they will be automatically taken into account.

Whenever you are satisfied with the release notes generated you can commit generated changes/new files
to the repository.

Before preparing the release, you must also update the version to release in the
`backport_packages/setup_backport_packages.py` - this is needed because you should add rc1/rc2 etc.
before releasing final version and only when you are ready, the final version should be released and updated
in the `backport_packages/setup_backport_packages.py`.

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

* PROVIDERS_CHANGES_YYYY.MM.DD.md which keeps information about changes (commits) in a particular
  version of the provider package. The file for latest release gets updated when you iterate with
  the same new date/version, but it never changes automatically for already released packages.
  This way - just before the final release, you can manually correct the changes file if you
  want to remove some changes from the file.

* README.md which is regenerated every time you run the script (unless there are no changes since
  the last time you generated the release notes

Note that our CI system builds the release notes for backport packages automatically with every build and
current date - this way you might be sure the automated generation of the release notes continues to
work. You can also preview the generated readme files (by downloading artifacts uploaded to file.io).
The script does not modify the README and CHANGES files if there is no change in the repo for that provider.

# Preparing backport packages

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
./breeze prepare-backport-packages -- --help
```

* To build the release packages (release candidates) run the following command:

```bash
./breeze prepare-backport-packages --version-suffux=rc1 -- [PACKAGE_ID] ...
```

for example:

```bash
./breeze prepare-backport-packages --version-suffix=rc1 -- http ...
```

* To build the release packages run the following command:

```bash
./breeze prepare-backport-packages -- [PACKAGE_ID] ...
```
for example:

```bash
./breeze prepare-backport-packages -- http ...
```

You can also build all packages by omitting the package id altogether.

* This creates a wheel package and source distribution packages in your `dist` folder with
  names similar to the below:

  * `apache_airflow_backport_providers_<PROVIDER>_YYYY.MM.DD-py2.py3-none-any.whl`
  * `apache-airflow-backport-providers-<PROVIDER>-YYYY.MM.DD.tar.gz`

* You can install this package with `pip install <PACKAGE_FILE>`


# Releasing the packages

## Building an RC

The Release Candidate artifacts we vote upon should be the exact ones we vote against, without any
modification than renaming – i.e. the contents of the files must be the same between voted
release candidate and final release. Because of this the version in the built artifacts
that will become the official Apache releases must not include the rcN suffix.

Note! that the version in PyPI does not contain the leading 0s in version name.

* Set environment variables (version and root of airflow repo)

```bash
export VERSION=2020.5.19rc2
export AIRFLOW_REPO_ROOT=$(pwd)

```

* Tag the release

```bash
git tag ${VERSION}
```

* Clean the checkout

```bash
git clean -fxd
```

* Tarball the sources

```bash
git archive --format=tar.gz ${VERSION} --prefix=apache-airflow-${VERSION}/ -o apache-airflow-backport-providers-${VERSION}-source.tar.gz
```

* Generate the packages (specify the version suffix, optionally list of packages).

This will clean up dist folder before generating the packages

```bash
./breeze prepare-backport-packages --version-suffix rc2 -- [PACKAGE ...]
```

* Move the source tarball to dist folder

```bash
mv apache-airflow-backport-providers-${VERSION}-source.tar.gz dist
```

* Sign all your packages

```bash
cd dist
../dev/sign.sh *.tar.gz *.whl
```

* Push tags

```bash
git push --tags
```

* Push the artifacts to ASF dev dist repo

```bash
# First clone the repo
svn checkout https://dist.apache.org/repos/dist/dev/airflow airflow-dev

# Create a new folder for the release
cd airflow-dev/backport-providers
svn mkdir ${VERSION}

# Move the artifacts to svn folder & commit
mv ${AIRFLOW_REPO_ROOT}/dist/apache{-,_}airflow-${VERSION}* ${VERSION}/
mv ${AIRFLOW_REPO_ROOT}/apache-airflow-backport-providers-${VERSION}-source.tar.gz ${VERSION}/

cd ${VERSION}
svn add *
svn commit -m "Add artifacts for Airflow ${VERSION}"
cd ${AIRFLOW_REPO_ROOT}
```

## Make sure your public key is on id.apache.org and in KEYS

You will need to sign the release artifacts with your pgp key. After you have created a key, make sure you:

* Add your GPG pub key to [KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS) ,
  follow the instructions at the top of that file. Upload your GPG public key to https://pgp.mit.edu
* Add your key fingerprint to [https://id.apache.org]/https://id.apache.org/ (login with your apache
  credentials, paste your fingerprint into the pgp fingerprint field and hit save).

```bash
# Create PGP Key
gpg --gen-key

# Checkout ASF dist repo
svn checkout https://dist.apache.org/repos/dist/release/airflow
cd airflow


# Add your GPG pub key to KEYS file. Replace "Kaxil Naik" with your name
(gpg --list-sigs "Kaxil Naik" && gpg --armor --export "Kaxil Naik" ) >> KEYS


# Commit the changes
svn commit -m "Add PGP keys of Airflow developers"
```


## Publishing to PyPi
Create a ~/.pypirc file:

```bash
[distutils]
index-servers =
  pypi
  pypitest

[pypi]
username=your-username

[pypitest]
repository=https://test.pypi.org/legacy/
username=your-username
```

Set proper permissions for the pypirc file:

```bash
$ chmod 600 ~/.pypirc
```

*  Install twine if you do not have it already.

```bash
pip install twine
```

* Verify the artifacts that would be uploaded:

```bash
twine check dist/*
```

* Upload the package to PyPi's test environment:

```bash
twine upload -r pypitest dist/*
```

* Verify that the test packages looks good by downloading it and installing them into a virtual environment.
Twine prints the package linksas output - separately for each package.

* Upload the package to PyPi's production environment:

```bash
twine upload -r pypi dist/*
```

Copy the list of links to the uploaded packages - they will be useful in preparing VOTE email.

* Again, confirm that the packages are available under the links displayed.

## Voting on RC candidates

Make sure the packages are in https://dist.apache.org/repos/dist/dev/airflow/backport-providers/

Use the dev/airflow-jira script to generate a list of Airflow JIRAs that were closed in the release.

Send out a vote to the dev@airflow.apache.org mailing list:

```text
[VOTE] Airflow Backport Providers 2020.5.19rc2

Hey all,

I have cut Airflow Backport Providers 2020.5.19rc2. This email is calling a vote on the release,
which will last for 72 hours - which means that it will end on YYYY,MM.DD TZN.

Consider this my (binding) +1.

Airflow Backport Providers 2020.5.19rc2 are available at:
https://dist.apache.org/repos/dist/dev/airflow/backport-providers/2020.5.19/

*apache-airflow-backport-providers-2020.5.19rc2-source.tar.gz* is a source release that comes
with INSTALL instructions.

*apache-airflow-backport-providers-PROVIDER-2020.5.19rc2-bin.tar.gz* is the binary Python "sdist" release.

Public keys are available at:
https://dist.apache.org/repos/dist/release/airflow/KEYS

Only votes from PMC members are binding, but members of the community are
encouraged to test the release and vote with "(non-binding)".

Please note that the version number excludes the `rcX` string, so it's now
simply 2020.5.19. This will allow us to rename the artifact without modifying
the artifact checksums when we actually release.

Each of the packages contains detailed changelog. Here is the list of links to
the released packages and changelogs:

TODO: Paste the result of twine upload

Cheers,
<your name>
```

* Once the vote has been passed, you will need to send a result vote to dev@airflow.apache.org:

```text
[RESULT][VOTE] Airflow Backport Providers 2020.5.19rc2


Hey all,

Airflow Backport Providers 2020.5.19 (based on RC2) has been accepted.

N “+1” binding votes received:
- PMC Member  (binding)
...

N "+1" non-binding votes received:

- COMMITER (non-binding)

Vote thread:
https://lists.apache.org/thread.html/NNNN@%3Cdev.airflow.apache.org%3E

I'll continue with the release process and the release announcement will follow shortly.

Cheers,
<your name>

```

## Publishing the final release

TODO:
