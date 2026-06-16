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
**Table of contents**

- [What the apache-airflow-mypy distribution is](#what-the-apache-airflow-mypy-distribution-is)
- [Decide when to release](#decide-when-to-release)
- [Versioning](#versioning)
- [Prepare Regular apache-airflow-mypy distributions (RC)](#prepare-regular-apache-airflow-mypy-distributions-rc)
  - [Generate release notes](#generate-release-notes)
  - [Build apache-airflow-mypy distributions for SVN apache upload](#build-apache-airflow-mypy-distributions-for-svn-apache-upload)
  - [Build and sign the source and convenience packages](#build-and-sign-the-source-and-convenience-packages)
  - [Add tags in git](#add-tags-in-git)
  - [Commit the source packages to Apache SVN repo](#commit-the-source-packages-to-apache-svn-repo)
  - [Publish the distributions to PyPI (release candidates)](#publish-the-distributions-to-pypi-release-candidates)
  - [Prepare voting email](#prepare-voting-email)
  - [Verify the release candidate by PMC members](#verify-the-release-candidate-by-pmc-members)
  - [Verify the release candidate by Contributors](#verify-the-release-candidate-by-contributors)
- [Publish release](#publish-release)
  - [Summarize the voting](#summarize-the-voting)
  - [Publish release to SVN](#publish-release-to-svn)
  - [Publish the packages to PyPI](#publish-the-packages-to-pypi)
  - [Add tags in git](#add-tags-in-git-1)
  - [Notify developers of release](#notify-developers-of-release)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

------------------------------------------------------------------------------------------------------------

# What the apache-airflow-mypy distribution is

The `apache-airflow-mypy` package provides Mypy plugins for Apache Airflow to enhance type checking capabilities.
It includes plugins for typed decorators and operator output type handling.

The Release Manager prepares `apache-airflow-mypy` packages separately from the main Airflow Release, using
`breeze` commands and accompanying scripts. This document provides an overview of the command line tools
needed to prepare the packages.

# Decide when to release

You can release `apache-airflow-mypy` distributions separately from the main Airflow on an ad-hoc basis,
whenever we find that the mypy plugins need to be released - due to new features, bug fixes, or improvements
to type checking support.

# Versioning

We are using the [SEMVER](https://semver.org/) versioning scheme for the `apache-airflow-mypy` distributions.
This is to give users confidence about maintaining backwards compatibility in new releases.

- **Major version** (X.0.0): Breaking changes to plugin interfaces or behavior
- **Minor version** (0.X.0): New features, new plugins, or significant enhancements
- **Patch version** (0.0.X): Bug fixes and minor improvements

# Prepare Regular apache-airflow-mypy distributions (RC)

## Generate release notes

Before releasing, update the `RELEASE_NOTES.rst` file with the changes since the last release.
You can use towncrier to generate release notes from newsfragments:

```bash
cd dev/mypy
towncrier build --version <VERSION>
```

To preview the release notes without writing to the file:

```bash
towncrier build --version <VERSION> --draft
```

Review and edit the generated release notes as needed.

## Build apache-airflow-mypy distributions for SVN apache upload

The distributions uploaded to the ASF dist SVN repo might get promoted to "final" packages by just
renaming the files, so internally they should keep the final version number **without** the rc suffix,
even if they are rc1/rc2/... candidates. Build them without a version suffix:

```bash
breeze release-management prepare-mypy-distributions \
    --distribution-format both \
    --version-suffix ""
```

This produces `apache_airflow_mypy-<VERSION>-py3-none-any.whl` and `apache_airflow_mypy-<VERSION>.tar.gz`
(no `rc` in the file names or internal metadata). The rc-suffixed packages are built separately for PyPI
(see [Publish the distributions to PyPI](#publish-the-distributions-to-pypi-release-candidates)).

## Build and sign the source and convenience packages

Follow the same signing process as other Airflow packages:

```bash
cd dist
for file in *.tar.gz *.whl; do
    gpg --armor --detach-sign $file
    sha512sum $file > $file.sha512
done
```

## Add tags in git

Tag the release candidate in git:

```bash
git tag -s apache-airflow-mypy-<VERSION>rc<RC> -m "Apache Airflow Mypy <VERSION>rc<RC>"
git push origin apache-airflow-mypy-<VERSION>rc<RC>
```

## Commit the source packages to Apache SVN repo

Upload the (non-suffixed) artifacts to the ASF dev dist repo under the
`apache-airflow-mypy/<VERSION>rc<RC>/` directory (the project has its own
group directory, the version-rc string is the folder name):

```bash
# Check out the dev dist area (shallow) if you do not have it yet
svn checkout --depth=immediates https://dist.apache.org/repos/dist/dev/airflow asf-dist-dev-airflow
cd asf-dist-dev-airflow

# Create the group/version-rc folder and move the signed artifacts in
mkdir -p apache-airflow-mypy/<VERSION>rc<RC>
mv ${AIRFLOW_REPO_ROOT}/dist/apache_airflow_mypy-<VERSION>* apache-airflow-mypy/<VERSION>rc<RC>/

# Remove any previous release candidate folders for this version
svn rm apache-airflow-mypy/<old-rc-folders>   # if applicable

svn add apache-airflow-mypy
svn commit -m "Add artifacts for Apache Airflow Mypy <VERSION>rc<RC>"
```

Verify the files appear at
https://dist.apache.org/repos/dist/dev/airflow/apache-airflow-mypy/<VERSION>rc<RC>/

## Publish the distributions to PyPI (release candidates)

Release candidates are published to PyPI as pre-releases (not TestPyPI), so voters can install them
directly with `pip install --pre`. These are **different** packages than the ones uploaded to SVN: they
carry the `rc` suffix, so build them with `--version-suffix rc<RC>`:

```bash
breeze release-management prepare-mypy-distributions \
    --distribution-format both \
    --version-suffix rc<RC>
twine upload dist/apache_airflow_mypy-<VERSION>rc<RC>*
```

Use a short-lived (throw-away) PyPI API token for the upload and delete it afterwards.

## Prepare voting email

Send a voting email to dev@airflow.apache.org with the following template:

```
Subject: [VOTE] Release Apache Airflow Mypy <VERSION> based on <VERSION>rc<RC>

Hello Apache Airflow Community,

This is a call for the vote to release Apache Airflow Mypy version <VERSION>.

The release candidate is available at:
https://dist.apache.org/repos/dist/dev/airflow/apache-airflow-mypy/<VERSION>rc<RC>/

The packages are available at PyPI:
https://pypi.org/project/apache-airflow-mypy/<VERSION>rc<RC>/

You can install the release candidate with:

    pip install --pre apache-airflow-mypy==<VERSION>rc<RC>

Public keys are available at:
https://dist.apache.org/repos/dist/release/airflow/KEYS

The vote will be open for at least 72 hours.

[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not release (please provide specific comments)

Only PMC votes are binding, but everyone is welcome to check and vote.

Best regards,
<YOUR NAME>
```

## Verify the release candidate by PMC members

PMC members should verify the integrity and provenance of the SVN artifacts before casting a binding
vote. The artifacts are versioned without the `rc` suffix (`apache_airflow_mypy-<VERSION>-*`) and live
in the `apache-airflow-mypy/<VERSION>rc<RC>/` sub-folder of the
[Airflow dev dist area](https://dist.apache.org/repos/dist/dev/airflow/apache-airflow-mypy).

### Setup Verification

Go to the directory where your Airflow sources are checked out and set the following environment
variables:

```shell script
export AIRFLOW_REPO_ROOT="$(pwd -P)"
VERSION=0.1.0
VERSION_SUFFIX=rc1
VERSION_RC=${VERSION}${VERSION_SUFFIX}
```

Check out the SVN dist area (as a PMC member you can clone it) and export the path to it. Do this from
the parent of your Airflow checkout:

```shell script
cd ..
[ -d asf-dist ] || svn checkout --depth=immediates https://dist.apache.org/repos/dist asf-dist
svn update --set-depth=infinity asf-dist/dev/airflow/apache-airflow-mypy

export PATH_TO_AIRFLOW_SVN="${PWD}/asf-dist/dev/airflow/"
```

Or update it if you already have it checked out:

```shell script
svn update "${PATH_TO_AIRFLOW_SVN}/apache-airflow-mypy"
```

### SVN check

The following 6 files should be present in the release directory (`.tar.gz` and `-py3-none-any.whl`,
each with `.asc` + `.sha512`):

```shell script
cd "${PATH_TO_AIRFLOW_SVN}/apache-airflow-mypy/${VERSION_RC}"
ls -la
```

Output should be like below

```shell
(apache-airflow) bugra@dev:~/PycharmProjects/asf-dist/dev/airflow/apache-airflow-mypy/0.1.0rc2$ ls -la
total 48
drwxrwxr-x 2 bugra bugra  4096 Jun 10 19:00 .
drwxrwxr-x 3 bugra bugra  4096 Jun 10 19:00 ..
-rw-rw-r-- 1 bugra bugra 11070 Jun 10 19:00 apache_airflow_mypy-0.1.0-py3-none-any.whl
-rw-rw-r-- 1 bugra bugra   906 Jun 10 19:00 apache_airflow_mypy-0.1.0-py3-none-any.whl.asc
-rw-rw-r-- 1 bugra bugra   173 Jun 10 19:00 apache_airflow_mypy-0.1.0-py3-none-any.whl.sha512
-rw-rw-r-- 1 bugra bugra  9116 Jun 10 19:00 apache_airflow_mypy-0.1.0.tar.gz
-rw-rw-r-- 1 bugra bugra   906 Jun 10 19:00 apache_airflow_mypy-0.1.0.tar.gz.asc
-rw-rw-r-- 1 bugra bugra   163 Jun 10 19:00 apache_airflow_mypy-0.1.0.tar.gz.sha512
```

### Signature check

Import the Airflow KEYS file (it must contain the Release Manager's key):

```shell script
wget -q https://dist.apache.org/repos/dist/release/airflow/KEYS -O /tmp/airflow-KEYS
gpg --import /tmp/airflow-KEYS
```

Then verify each signature:

```shell script
cd "${PATH_TO_AIRFLOW_SVN}/apache-airflow-mypy/${VERSION_RC}"
for i in *.asc
do
   echo -e "Checking $i\n"; gpg --verify $i
done
```

The "Good signature from ..." line indicates the signatures are correct. The "not certified with a
trusted signature" warning is expected for self-signed release-manager keys.

### SHA512 check

```shell script
cd "${PATH_TO_AIRFLOW_SVN}/apache-airflow-mypy/${VERSION_RC}"
for i in *.sha512
do
    echo "Checking $i"; shasum -a 512 `basename $i .sha512` | diff - $i
done
```

### Reproducible package builds check

`apache-airflow-mypy` supports reproducible builds: packages built from the same sources are binary
identical. Rebuild from the tagged source and confirm the packages match the SVN ones. Build
**without** a version suffix so the file names and embedded metadata match the (non-suffixed) SVN
artifacts.

Check out the tag (the commands assume the standard remote naming convention `upstream` →
`apache/airflow`):

```shell script
cd "${AIRFLOW_REPO_ROOT}"
git fetch upstream --tags
git checkout "apache-airflow-mypy-${VERSION_RC}"
rm -rf dist/*
breeze release-management prepare-mypy-distributions --distribution-format both --version-suffix ""
```

Then compare the packages you built with the ones in SVN:

```shell script
cd "${PATH_TO_AIRFLOW_SVN}/apache-airflow-mypy/${VERSION_RC}"
for i in ${AIRFLOW_REPO_ROOT}/dist/*
do
  echo "Checking if $(basename $i) is the same as $i"
  diff "$(basename $i)" "$i" && echo "OK"
done
```

The output should show `OK` for every file (they are identical).

### Licence check

This can be done with the Apache RAT tool. There should be no files reported as `Unknown` or
`Unapproved`.

Download the latest jar (including checksum verification for your own security):

```shell script
# Checksum value is taken from https://downloads.apache.org/creadur/apache-rat-0.18/apache-rat-0.18-bin.tar.gz.sha512
wget -q https://archive.apache.org/dist/creadur/apache-rat-0.18/apache-rat-0.18-bin.tar.gz -O /tmp/apache-rat-0.18-bin.tar.gz
echo "315b16536526838237c42b5e6b613d29adc77e25a6e44a866b2b7f8b162e03d3629d49c9faea86ceb864a36b2c42838b8ce43d6f2db544e961f2259e242748f4  /tmp/apache-rat-0.18-bin.tar.gz" | sha512sum -c -
tar -xzf /tmp/apache-rat-0.18-bin.tar.gz -C /tmp
```

Unpack the sdist and run the check, reusing the repository `.rat-excludes` file:

```shell script
rm -rf /tmp/apache-airflow-mypy-src && mkdir -p /tmp/apache-airflow-mypy-src
tar -xzf "${PATH_TO_AIRFLOW_SVN}/apache-airflow-mypy/${VERSION_RC}/apache_airflow_mypy-${VERSION}.tar.gz" --strip-components 1 -C /tmp/apache-airflow-mypy-src
cp "${AIRFLOW_REPO_ROOT}/.rat-excludes" /tmp/apache-airflow-mypy-src/.rat-excludes
java -jar /tmp/apache-rat-0.18/apache-rat-0.18.jar --input-exclude-file /tmp/apache-airflow-mypy-src/.rat-excludes /tmp/apache-airflow-mypy-src/
```

You should see no files reported as `Unknown` or with wrong licence.

## Verify the release candidate by Contributors

Contributors (and especially actual users) are encouraged to test the release candidate functionally.
Each candidate is published to PyPI as a pre-release, so anyone can install it:

### Installing in your local virtualenv

```shell script
pip install --pre apache-airflow-mypy==<VERSION>rc<RC>
```

### Testing the plugins

Add the plugins to your mypy configuration and run mypy against your code base:

```ini
[mypy]
plugins = airflow_mypy.plugins.decorators, airflow_mypy.plugins.outputs
```

Run the test suite if available, and perform any additional verification you see as necessary.

# Publish release

## Summarize the voting

Once the vote passes, summarize the results in a reply to the voting thread.

## Publish release to SVN

Move the release from dev to release in SVN. Because the artifacts are already versioned without the
`rc` suffix, promotion is a simple move of the version-rc folder to the final version folder:

```bash
svn mv https://dist.apache.org/repos/dist/dev/airflow/apache-airflow-mypy/<VERSION>rc<RC> \
       https://dist.apache.org/repos/dist/release/airflow/apache-airflow-mypy/<VERSION> \
       -m "Release Apache Airflow Mypy <VERSION>"
```

## Publish the packages to PyPI

Publish the final release to PyPI:

```bash
twine upload dist/apache_airflow_mypy-<VERSION>-py3-none-any.whl
twine upload dist/apache_airflow_mypy-<VERSION>.tar.gz
```

## Add tags in git

Tag the final release:

```bash
git tag -s apache-airflow-mypy-<VERSION> -m "Apache Airflow Mypy <VERSION>"
git push origin apache-airflow-mypy-<VERSION>
```

## Notify developers of release

Send an announcement email to dev@airflow.apache.org and announce@apache.org:

```
Subject: [ANNOUNCE] Apache Airflow Mypy <VERSION> released

The Apache Airflow team is pleased to announce the release of Apache Airflow Mypy <VERSION>.

Apache Airflow Mypy provides Mypy plugins for Apache Airflow to enhance type checking capabilities.

The release is available at:
https://pypi.org/project/apache-airflow-mypy/<VERSION>/

Release notes:
https://github.com/apache/airflow/blob/main/dev/mypy/RELEASE_NOTES.rst

Installation:
pip install apache-airflow-mypy

Usage:
Add to your mypy configuration:
[mypy]
plugins = airflow_mypy.plugins.decorators, airflow_mypy.plugins.outputs

Cheers,
The Apache Airflow Team
```
