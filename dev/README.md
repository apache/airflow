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

- [Apache Airflow source releases](#apache-airflow-source-releases)
  - [Apache Airflow Package](#apache-airflow-package)
  - [Backport Provider packages](#backport-provider-packages)
- [Prerequisites for the release manager preparing the release](#prerequisites-for-the-release-manager-preparing-the-release)
  - [Upload Public keys to id.apache.org](#upload-public-keys-to-idapacheorg)
  - [Configure PyPI uploads](#configure-pypi-uploads)
  - [Hardware used to prepare and verify the packages](#hardware-used-to-prepare-and-verify-the-packages)
- [Apache Airflow packages](#apache-airflow-packages)
  - [Prepare the Apache Airflow Package RC](#prepare-the-apache-airflow-package-rc)
  - [Vote and verify the Apache Airflow release candidate](#vote-and-verify-the-apache-airflow-release-candidate)
  - [Publish the final Apache Airflow release](#publish-the-final-apache-airflow-release)
- [Provider Packages](#provider-packages)
  - [Decide when to release](#decide-when-to-release)
  - [Prepare the Backport Provider Packages RC](#prepare-the-backport-provider-packages-rc)
  - [Vote and verify the Backport Providers release candidate](#vote-and-verify-the-backport-providers-release-candidate)
  - [Publish the final releases of backport packages](#publish-the-final-releases-of-backport-packages)
  - [Prepare the Regular Provider Packages Alpha](#prepare-the-regular-provider-packages-alpha)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Apache Airflow source releases

The Apache Airflow releases are one of the two types:

* Releases of the Apache Airflow package
* Releases of the Backport Providers Packages

## Apache Airflow Package

This package contains sources that allow the user building fully-functional Apache Airflow 2.0 package.
They contain sources for:

 * "apache-airflow" python package that installs "airflow" Python package and includes
   all the assets required to release the webserver UI coming with Apache Airflow
 * Dockerfile and corresponding scripts that build and use an official DockerImage
 * Breeze development environment that helps with building images and testing locally
   apache airflow built from sources

In the future (Airflow 2.0) this package will be split into separate "core" and "providers" packages that
will be distributed separately, following the mechanisms introduced in Backport Package Providers. We also
plan to release the official Helm Chart sources that will allow the user to install Apache Airflow
via helm 3.0 chart in a distributed fashion.

The Source releases are the only "official" Apache Software Foundation releases, and they are distributed
via [Official Apache Download sources](https://downloads.apache.org/)

Following source releases Apache Airflow release manager also distributes convenience packages:

* PyPI packages released via https://pypi.org/project/apache-airflow/
* Docker Images released via https://hub.docker.com/repository/docker/apache/airflow

Those convenience packages are not "official releases" of Apache Airflow, but the users who
cannot or do not want to build the packages themselves can use them as a convenient way of installing
Apache Airflow, however they are not considered as "official source releases". You can read more
details about it in the [ASF Release Policy](http://www.apache.org/legal/release-policy.html).

This document describes the process of releasing both - official source packages and convenience
packages for Apache Airflow packages.

## Backport Provider packages

The Backport Provider packages are packages (per provider) that make it possible to easily use Hooks,
Operators, Sensors, and Secrets from the 2.0 version of Airflow in the 1.10.* series.

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

The sources released in SVN allow to build all the provider packages by the user, following the
instructions and scripts provided. Those are also "official_source releases" as described in the
[ASF Release Policy](http://www.apache.org/legal/release-policy.html) and they are available
via [Official Apache Download sources](https://downloads.apache.org/airflow/backport-providers/).

There are also 50+ convenience packages released as "apache-airflow-backport-providers" separately in
PyPI. You can find them all by [PyPI query](https://pypi.org/search/?q=apache-airflow-backport-providers)

The document describes the process of releasing both - official source packages and convenience
packages for Backport Provider Packages.

# Prerequisites for the release manager preparing the release

The person acting as release manager has to fulfill certain pre-requisites. More details and FAQs are
available in the [ASF Release Policy](http://www.apache.org/legal/release-policy.html) but here some important
pre-requisites are listed below. Note that release manager does not have to be a PMC - it is enough
to be committer to assume the release manager role, but there are final steps in the process (uploading
final releases to SVN) that can only be done by PMC member. If needed, the release manager
can ask PMC to perform that final step of release.

## Upload Public keys to id.apache.org

Make sure your public key is on id.apache.org and in KEYS. You will need to sign the release artifacts
with your pgp key. After you have created a key, make sure you:

- Add your GPG pub key to https://dist.apache.org/repos/dist/release/airflow/KEYS , follow the instructions at the top of that file. Upload your GPG public key to https://pgp.mit.edu
- Add your key fingerprint to https://id.apache.org/ (login with your apache credentials, paste your fingerprint into the pgp fingerprint field and hit save).

```shell script
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

See this for more detail on creating keys and what is required for signing releases.

http://www.apache.org/dev/release-signing.html#basic-facts

## Configure PyPI uploads

In order to not reveal your password in plain text, it's best if you create and configure API Upload tokens.
You can add and copy the tokens here:

* [Test PyPI](https://test.pypi.org/manage/account/token/)
* [Prod PyPI](https://pypi.org/manage/account/token/)


Create a `~/.pypirc` file:

```ini
[distutils]
index-servers =
  pypi
  pypitest

[pypi]
username=__token__
password=<API Upload Token>

[pypitest]
repository=https://test.pypi.org/legacy/
username=__token__
password=<API Upload Token>
```

Set proper permissions for the pypirc file:

```shell script
$ chmod 600 ~/.pypirc
```

- Install [twine](https://pypi.org/project/twine/) if you do not have it already (it can be done
  in a separate virtual environment).

```shell script
pip install twine
```

(more details [here](https://peterdowns.com/posts/first-time-with-pypi.html).)

- Set proper permissions for the pypirc file:
`$ chmod 600 ~/.pypirc`

- Confirm that `airflow/version.py` is set properly.


## Hardware used to prepare and verify the packages

The best way to prepare and verify the releases is to prepare them on a hardware owned and controlled
by the committer acting as release manager. While strictly speaking, releases must only be verified
on hardware owned and controlled by the committer, for practical reasons it's best if the packages are
prepared using such hardware. More information can be found in this
[FAQ](http://www.apache.org/legal/release-policy.html#owned-controlled-hardware)

# Apache Airflow packages

## Prepare the Apache Airflow Package RC

### Build RC artifacts (both source packages and convenience packages)

The Release Candidate artifacts we vote upon should be the exact ones we vote against, without any modification than renaming – i.e. the contents of the files must be the same between voted release canidate and final release. Because of this the version in the built artifacts that will become the official Apache releases must not include the rcN suffix.

- Set environment variables

    ```shell script
    # Set Version
    export VERSION=1.10.2rc3


    # Set AIRFLOW_REPO_ROOT to the path of your git repo
    export AIRFLOW_REPO_ROOT=$(pwd)


    # Example after cloning
    git clone https://github.com/apache/airflow.git airflow
    cd airflow
    export AIRFLOW_REPO_ROOT=$(pwd)
    ```

- Set your version to 1.10.2 in `airflow/version.py` (without the RC tag)
- Commit the version change.

- Tag your release

    ```shell script
    git tag ${VERSION}
    ```

- Clean the checkout: the sdist step below will

    ```shell script
    git clean -fxd
    ```

- Tarball the repo

    ```shell script
    git archive --format=tar.gz ${VERSION} --prefix=apache-airflow-${VERSION}/ -o apache-airflow-${VERSION}-source.tar.gz`
    ```


- Generate sdist

    NOTE: Make sure your checkout is clean at this stage - any untracked or changed files will otherwise be included
     in the file produced.

    ```shell script
    python setup.py compile_assets sdist bdist_wheel
    ```

- Rename the sdist

    ```shell script
    mv dist/apache-airflow-${VERSION%rc?}.tar.gz apache-airflow-${VERSION}-bin.tar.gz
    mv dist/apache_airflow-${VERSION%rc?}-py2.py3-none-any.whl apache_airflow-${VERSION}-py2.py3-none-any.whl
    ```

- Generate SHA512/ASC (If you have not generated a key yet, generate it by following instructions on http://www.apache.org/dev/openpgp.html#key-gen-generate-key)

    ```shell script
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh apache-airflow-${VERSION}-source.tar.gz
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh apache-airflow-${VERSION}-bin.tar.gz
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh apache_airflow-${VERSION}-py2.py3-none-any.whl
    ```

- Push Tags
`git push --tags`

- Push the artifacts to ASF dev dist repo
```
# First clone the repo
svn checkout https://dist.apache.org/repos/dist/dev/airflow airflow-dev

# Create new folder for the release
cd airflow-dev
svn mkdir ${VERSION}

# Move the artifacts to svn folder & commit
mv ${AIRFLOW_REPO_ROOT}/apache{-,_}airflow-${VERSION}* ${VERSION}/
cd ${VERSION}
svn add *
svn commit -m "Add artifacts for Airflow ${VERSION}"
```

### Prepare PyPI convenience "snapshot" packages

At this point we have the artefact that we vote on, but as a convenience to developers we also want to
publish "snapshots" of the RC builds to pypi for installing via pip. To do this we need to

- Edit the `airflow/version.py` to include the RC suffix.

- Build the package:

    ```shell script
    python setup.py compile_assets sdist bdist_wheel
    ```

- Verify the artifacts that would be uploaded:

    ```shell script
    twine check dist/*
    ```

- Upload the package to PyPi's test environment:

    ```shell script
    twine upload -r pypitest dist/*
    ```

- Verify that the test package looks good by downloading it and installing it into a virtual environment. The package download link is available at:
https://test.pypi.org/project/apache-airflow/#files

- Upload the package to PyPi's production environment:
`twine upload -r pypi dist/*`

- Again, confirm that the package is available here:
https://pypi.python.org/pypi/apache-airflow

- Throw away the change - we don't want to commit this: `git checkout airflow/version.py`

It is important to stress that this snapshot should not be named "release", and it
is not supposed to be used by and advertised to the end-users who do not read the devlist.

## Vote and verify the Apache Airflow release candidate

### Prepare Vote email on the Apache Airflow release candidate

- Use the dev/airflow-jira script to generate a list of Airflow JIRAs that were closed in the release.

- Send out a vote to the dev@airflow.apache.org mailing list:

Subject:
```
[VOTE] Airflow 1.10.2rc3
```

Body:

```
Hey all,

I have cut Airflow 1.10.2 RC3. This email is calling a vote on the release,
which will last for 72 hours. Consider this my (binding) +1.

Airflow 1.10.2 RC3 is available at:
https://dist.apache.org/repos/dist/dev/airflow/1.10.2rc3/

*apache-airflow-1.10.2rc3-source.tar.gz* is a source release that comes
with INSTALL instructions.
*apache-airflow-1.10.2rc3-bin.tar.gz* is the binary Python "sdist" release.

Public keys are available at:
https://dist.apache.org/repos/dist/release/airflow/KEYS

Only votes from PMC members are binding, but the release manager should encourage members of the community
to test the release and vote with "(non-binding)".

The test procedure for PMCs and Contributors who would like to test this RC are described in
https://github.com/apache/airflow/blob/master/dev/README.md#vote-and-verify-the-apache-airflow-release-candidate

Please note that the version number excludes the `rcX` string, so it's now
simply 1.10.2. This will allow us to rename the artifact without modifying
the artifact checksums when we actually release.


Changes since 1.10.2rc2:
*Bugs*:
[AIRFLOW-3732] Fix issue when trying to edit connection in RBAC UI
[AIRFLOW-2866] Fix missing CSRF token head when using RBAC UI (#3804)
...


*Improvements*:
[AIRFLOW-3302] Small CSS fixes (#4140)
[Airflow-2766] Respect shared datetime across tabs
...


*New features*:
[AIRFLOW-2874] Enables FAB's theme support (#3719)
[AIRFLOW-3336] Add new TriggerRule for 0 upstream failures (#4182)
...


*Doc-only Change*:
[AIRFLOW-XXX] Fix BashOperator Docstring (#4052)
[AIRFLOW-3018] Fix Minor issues in Documentation
...

Cheers,
<your name>
```

### Verify the release candidate by PMCs (legal)

#### PMC responsibilities

The PMCs should verify the releases in order to make sure the release is following the
[Apache Legal Release Policy](http://www.apache.org/legal/release-policy.html).

At least 3 (+1) votes should be recorded in accordance to
[Votes on Package Releases](https://www.apache.org/foundation/voting.html#ReleaseVotes)

The legal checks include:

* checking if the packages are present in the right dist folder on svn
* verifying if all the sources have correct licences
* verifying if release manager signed the releases with the right key
* verifying if all the checksums are valid for the release

#### SVN check

The files should be present in the sub-folder of
[Airflow dist](https://dist.apache.org/repos/dist/dev/airflow/)

The following files should be present (9 files):

* -bin-tar.gz + .asc + .sha512
* -source.tar.gz + .asc + .sha512
* -.whl + .asc + .sha512

As a PMC you should be able to clone the SVN repository:

```shell script
svn co https://dist.apache.org/repos/dist/dev/airflow
```

Or update it if you already checked it out:

```shell script
svn update .
```

#### Verify the licences

This can be done with the Apache RAT tool.

* Download the latest jar from https://creadur.apache.org/rat/download_rat.cgi (unpack the sources,
  the jar is inside)
* Unpack the -source.tar.gz to a folder
* Enter the folder and run the check (point to the place where you extracted the .jar)

```shell script
java -jar ../../apache-rat-0.13/apache-rat-0.13.jar -E .rat-excludes -d .
```

#### Verify the signatures

Make sure you have the key of person signed imported in your GPG. You can find the valid keys in
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS).

You can import the whole KEYS file:

```shell script
gpg --import KEYS
```

You can also import the keys individually from a keyserver. The below one uses Kaxil's key and
retrieves it from the default GPG keyserver
[OpenPGP.org](https://keys.openpgp.org):

```shell script
gpg --receive-keys 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
```

You should choose to import the key when asked.

Note that by being default, the OpenPGP server tends to be overloaded often and might respond with
errors or timeouts. Many of the release managers also uploaded their keys to the
[GNUPG.net](https://keys.gnupg.net) keyserver, and you can retrieve it from there.

```shell script
gpg --keyserver keys.gnupg.net --receive-keys 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
```

Once you have the keys, the signatures can be verified by running this:

```shell script
for i in *.asc
do
   echo "Checking $i"; gpg --verify `basename $i .sha512 `
done
```

This should produce results similar to the below. The "Good signature from ..." is indication
that the signatures are correct. Do not worry about the "not certified with a trusted signature"
warning. Most of the certificates used by release managers are self signed, that's why you get this
warning. By importing the server in the previous step and importing it via ID from
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS) page, you know that
this is a valid Key already.

```
Checking apache-airflow-1.10.12rc4-bin.tar.gz.asc
gpg: assuming signed data in 'apache-airflow-1.10.12rc4-bin.tar.gz'
gpg: Signature made sob, 22 sie 2020, 20:28:28 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
Checking apache_airflow-1.10.12rc4-py2.py3-none-any.whl.asc
gpg: assuming signed data in 'apache_airflow-1.10.12rc4-py2.py3-none-any.whl'
gpg: Signature made sob, 22 sie 2020, 20:28:31 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
Checking apache-airflow-1.10.12rc4-source.tar.gz.asc
gpg: assuming signed data in 'apache-airflow-1.10.12rc4-source.tar.gz'
gpg: Signature made sob, 22 sie 2020, 20:28:25 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
```

#### Verify the SHA512 sum

Run this:

```shell script
for i in *.sha512
do
    echo "Checking $i"; gpg --print-md SHA512 `basename $i .sha512 ` | diff - $i
done
```

You should get output similar to:

```
Checking apache-airflow-1.10.12rc4-bin.tar.gz.sha512
Checking apache_airflow-1.10.12rc4-py2.py3-none-any.whl.sha512
Checking apache-airflow-1.10.12rc4-source.tar.gz.sha512
```

### Verify if the release candidate "works" by Contributors

This can be done (and we encourage to) by any of the Contributors. In fact, it's best if the
actual users of Apache Airflow test it in their own staging/test installations. Each release candidate
is available on PyPI apart from SVN packages, so everyone should be able to install
the release candidate version of Airflow via simply (<VERSION> is 1.10.12 for example, and <X> is
release candidate number 1,2,3,....).

```shell script
pip install apache-airflow==<VERSION>rc<X>
```
Optionally it can be followed with constraints

```shell script
pip install apache-airflow==<VERSION>rc<X> \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-<VERSION>/constraints-3.6.txt"`
```

Note that the constraints contain python version that you are installing it with.

You can use any of the installation methods you prefer (you can even install it via the binary wheel
downloaded from the SVN).

There is also an easy way of installation with Breeze if you have the latest sources of Apache Airflow.
Running the following command will use tmux inside breeze, create `admin` user and run Webserver & Scheduler:

```shell script
./breeze start-airflow --install-airflow-version <VERSION>rc<X> --python 3.7 --backend postgres
```

For 1.10 releases you can also use `--no-rbac-ui` flag disable RBAC UI of Airflow:

```shell script
./breeze start-airflow --install-airflow-version <VERSION>rc<X> --python 3.7 --backend postgres --no-rbac-ui
```

Once you install and run Airflow, you should perform any verification you see as necessary to check
that the Airflow works as you expected.

## Publish the final Apache Airflow release

### Summarize the voting for the Apache Airflow release

Once the vote has been passed, you will need to send a result vote to dev@airflow.apache.org:

Subject:
```
[RESULT][VOTE] Airflow 1.10.2rc3
```

Message:

```
Hello,

Apache Airflow 1.10.2 (based on RC3) has been accepted.

4 “+1” binding votes received:
- Kaxil Naik  (binding)
- Bolke de Bruin (binding)
- Ash Berlin-Taylor (binding)
- Tao Feng (binding)


4 "+1" non-binding votes received:

- Deng Xiaodong (non-binding)
- Stefan Seelmann (non-binding)
- Joshua Patchus (non-binding)
- Felix Uellendall (non-binding)

Vote thread:
https://lists.apache.org/thread.html/736404ca3d2b2143b296d0910630b9bd0f8b56a0c54e3a05f4c8b5fe@%3Cdev.airflow.apache.org%3E

I'll continue with the release process, and the release announcement will follow shortly.

Cheers,
<your name>
```


### Publish release to SVN

You need to migrate the RC artifacts that passed to this repository:
https://dist.apache.org/repos/dist/release/airflow/
(The migration should include renaming the files so that they no longer have the RC number in their filenames.)

The best way of doing this is to svn cp  between the two repos (this avoids having to upload the binaries again, and gives a clearer history in the svn commit logs):

```shell script
# First clone the repo
export RC=1.10.4rc5
export VERSION=${RC/rc?/}
svn checkout https://dist.apache.org/repos/dist/release/airflow airflow-release

# Create new folder for the release
cd airflow-release
svn mkdir ${VERSION}
cd ${VERSION}

# Move the artifacts to svn folder & commit
for f in ../../airflow-dev/$RC/*; do svn cp $f ${$(basename $f)/rc?/}; done
svn commit -m "Release Airflow ${VERSION} from ${RC}"

# Remove old release
# http://www.apache.org/legal/release-policy.html#when-to-archive
cd ..
export PREVIOUS_VERSION=1.10.1
svn rm ${PREVIOUS_VERSION}
svn commit -m "Remove old release: ${PREVIOUS_VERSION}"
```

Verify that the packages appear in [airflow](https://dist.apache.org/repos/dist/release/airflow/)

### Prepare PyPI "release" packages

At this point we release an official package:

- Build the package:

    ```shell script
    python setup.py compile_assets sdist bdist_wheel`
    ```

- Verify the artifacts that would be uploaded:

    ```shell script
    twine check dist/*`
    ```

- Upload the package to PyPi's test environment:

    ```shell script
    twine upload -r pypitest dist/*
    ```

- Verify that the test package looks good by downloading it and installing it into a virtual environment.
    The package download link is available at: https://test.pypi.org/project/apache-airflow/#files

- Upload the package to PyPi's production environment:

    ```shell script
    twine upload -r pypi dist/*
    ```

- Again, confirm that the package is available here: https://pypi.python.org/pypi/apache-airflow

### Update CHANGELOG.md

- Get a diff between the last version and the current version:

    ```shell script
    $ git log 1.8.0..1.9.0 --pretty=oneline
    ```
- Update CHANGELOG.md with the details, and commit it.

### Notify developers of release

- Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org and announce@apache.org) that
the artifacts have been published:

Subject:
```shell script
cat <<EOF
Airflow ${VERSION} is released
EOF
```

Body:
```shell script
cat <<EOF
Dear Airflow community,

I'm happy to announce that Airflow ${VERSION} was just released.

The source release, as well as the binary "sdist" release, are available
here:

https://dist.apache.org/repos/dist/release/airflow/${VERSION}/

We also made this version available on PyPi for convenience (`pip install apache-airflow`):

https://pypi.python.org/pypi/apache-airflow

The documentation is available on:
https://airflow.apache.org/
https://airflow.apache.org/1.10.2/
https://airflow.readthedocs.io/en/1.10.2/
https://airflow.readthedocs.io/en/stable/

Find the CHANGELOG here for more details:

https://airflow.apache.org/changelog.html#airflow-1-10-2-2019-01-19

Cheers,
<your name>
EOF
```

### Update Announcements page

Update "Announcements" page at the [Official Airflow website](https://airflow.apache.org/announcements/)


-----------------------------------------------------------------------------------------------------------


# Provider Packages

You can read more about the command line tools used to generate the packages and the two types of
packages we have (Backport and Regular Provider Packages) in [Provider packages](PROVIDER_PACKAGES.md).

## Decide when to release

You can release provider packages separately from the main Airflow on an ad-hoc basis, whenever we find that
a given provider needs to be released - due to new features or due to bug fixes.
You can release each provider package separately, but due to voting and release overhead we try to group
releases of provider packages together.

### Backport provider packages versioning

We are using the [CALVER](https://calver.org/) versioning scheme for the backport packages. We also have an
automated way to prepare and build the packages, so it should be very easy to release the packages often and
separately. Backport packages will be maintained for three months after 2.0.0 version of Airflow, and it is
really a bridge, allowing people to migrate to Airflow 2.0 in stages, so the overhead of maintaining
semver versioning does not apply there - subsequent releases might be backward-incompatible, and it is
not indicated by the version of the packages.

### Regular provider packages versioning

We are using the [SEMVER](https://semver.org/) versioning scheme for the regular packages. This is in order
to give the users confidence about maintaining backwards compatibility in the new releases of those
packages.

Details about maintaining the SEMVER version are going to be discussed and implemented in
[the related issue](https://github.com/apache/airflow/issues/11425)

## Prepare the Backport Provider Packages RC

### Generate release notes

Prepare release notes for all the packages you plan to release. Where YYYY.MM.DD is the CALVER
date for the packages.

```shell script
./breeze --backports prepare-provider-readme YYYY.MM.DD [packages]
```

If you iterate with merges and release candidates you can update the release date without providing
the date (to update the existing release notes)

```shell script
./breeze --backports prepare-provider-readme google
```

Generated readme files should be eventually committed to the repository.

### Build an RC release for SVN apache upload

The Release Candidate artifacts we vote upon should be the exact ones we vote against, without any
modification than renaming i.e. the contents of the files must be the same between voted
release candidate and final release. Because of this the version in the built artifacts
that will become the official Apache releases must not include the rcN suffix. They also need
to be signed and have checksum files. You can generate the checksum/signature files by running
the "dev/sign.sh" script (assuming you have the right PGP key set-up for signing). The script
generates corresponding .asc and .sha512 files for each file to sign.

#### Build and sign the source and convenience packages

* Set environment variables (version and root of airflow repo)

```shell script
export VERSION=2020.5.20rc2
export AIRFLOW_REPO_ROOT=$(pwd)

```

* Build the source package:

```shell script
./provider_packages/build_source_package.sh
```

It will generate `apache-airflow-backport-providers-${VERSION}-source.tar.gz`

* Generate the packages - since we are preparing packages for SVN repo, we should use the right switch. Note
  that this will clean up dist folder before generating the packages, so it will only contain the packages
  you intended to build.

```shell script
./breeze prepare-provider-packages --version-suffix-for-svn rc1
```

if you ony build few packages, run:

```shell script
./breeze prepare-provider-packages --version-suffix-for-svn rc1 PACKAGE PACKAGE ....
```

* Move the source tarball to dist folder

```shell script
mv apache-airflow-backport-providers-${VERSION}-source.tar.gz dist
```

* Sign all your packages

```shell script
pushd dist
../dev/sign.sh *
popd
```

* Push tags to Apache repository (assuming that you have apache remote pointing to apache/airflow repo)]

```shell script
git push apache backport-providers-${VERSION}
```

#### Commit the source packages to Apache SVN repo

* Push the artifacts to ASF dev dist repo

```shell script
# First clone the repo if you do not have it
svn checkout https://dist.apache.org/repos/dist/dev/airflow airflow-dev

# update the repo in case you have it already
cd airflow-dev
svn update

# Create a new folder for the release.
cd airflow-dev/backport-providers
svn mkdir ${VERSION}

# Move the artifacts to svn folder
mv ${AIRFLOW_REPO_ROOT}/dist/* ${VERSION}/

# Add and commit
svn add ${VERSION}/*
svn commit -m "Add artifacts for Airflow ${VERSION}"

cd ${AIRFLOW_REPO_ROOT}
```

Verify that the files are available at
[backport-providers](https://dist.apache.org/repos/dist/dev/airflow/backport-providers/)

### Publish the RC convenience package to PyPI

In order to publish to PyPI you just need to build and release packages. The packages should however
contain the rcN suffix in the version name as well, so you need to use `--version-suffix-for-pypi` switch
to prepare those packages. Note that these are different packages than the ones used for SVN upload
though they should be generated from the same sources.

* Generate the packages with the right RC version (specify the version suffix with PyPI switch). Note that
this will clean up dist folder before generating the packages, so you will only have the right packages there.

```shell script
./breeze prepare-provider-packages --version-suffix-for-pypi rc1
```

if you ony build few packages, run:

```shell script
./breeze prepare-provider-packages --version-suffix-for-pypi rc1 PACKAGE PACKAGE ....
```

* Verify the artifacts that would be uploaded:

```shell script
twine check dist/*
```

* Upload the package to PyPi's test environment:

```shell script
twine upload -r pypitest dist/*
```

* Verify that the test packages look good by downloading it and installing them into a virtual environment.
Twine prints the package links as output - separately for each package.

* Upload the package to PyPi's production environment:

```shell script
twine upload -r pypi dist/*
```

* Copy the list of links to the uploaded packages - they will be useful in preparing VOTE email.

* Again, confirm that the packages are available under the links printed.

## Vote and verify the Backport Providers release candidate

### Prepare voting email for Backport Providers release candidate

Make sure the packages are in https://dist.apache.org/repos/dist/dev/airflow/backport-providers/

Send out a vote to the dev@airflow.apache.org mailing list. Here you can prepare text of the
email using the ${VERSION} variable you already set in the command line.

subject:


```shell script
cat <<EOF
[VOTE] Airflow Backport Providers ${VERSION}
EOF
```

```shell script
cat <<EOF
Hey all,

I have cut Airflow Backport Providers ${VERSION}. This email is calling a vote on the release,
which will last for 72 hours - which means that it will end on $(date -d '+3 days').

Consider this my (binding) +1.

Airflow Backport Providers ${VERSION} are available at:
https://dist.apache.org/repos/dist/dev/airflow/backport-providers/${VERSION}/

*apache-airflow-backport-providers-${VERSION}-source.tar.gz* is a source release that comes
 with INSTALL instructions.

*apache-airflow-backport-providers-<PROVIDER>-${VERSION}-bin.tar.gz* are the binary
 Python "sdist" release.

The test procedure for PMCs and Contributors who would like to test the RC candidates are described in
https://github.com/apache/airflow/blob/master/dev/README.md#vote-and-verify-the-backport-providers-release-candidate


Public keys are available at:
https://dist.apache.org/repos/dist/release/airflow/KEYS

Please vote accordingly:

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason


Only votes from PMC members are binding, but members of the community are
encouraged to test the release and vote with "(non-binding)".

Please note that the version number excludes the 'rcX' string, so it's now
simply ${VERSION%rc?}. This will allow us to rename the artifact without modifying
the artifact checksums when we actually release.

Each of the packages contains detailed changelog. Here is the list of links to
the released packages and changelogs:

TODO: Paste the result of twine upload

Cheers,
<TODO: Your Name>

EOF
```

Due to the nature of backport packages, not all packages have to be released as convenience
packages in the final release. During the voting process
the voting PMCs might decide to exclude certain packages from the release if some critical
problems have been found in some packages.

Please modify the message above accordingly to clearly exclude those packages.

### Verify the release

#### SVN check

The files should be present in the sub-folder of
[Airflow dist](https://dist.apache.org/repos/dist/dev/airflow/backport-providers/)

The following files should be present (9 files):

* -source.tar.gz + .asc + .sha512 (one set of files)
* -bin-tar.gz + .asc + .sha512 (one set of files per provider)
* -.whl + .asc + .sha512 (one set of files per provider)

As a PMC you should be able to clone the SVN repository:

```shell script
svn co https://dist.apache.org/repos/dist/dev/airflow/
```

Or update it if you already checked it out:

```shell script
svn update .
```

#### Verify the licences

This can be done with the Apache RAT tool.

* Download the latest jar from https://creadur.apache.org/rat/download_rat.cgi (unpack the sources,
  the jar is inside)
* Unpack the -source.tar.gz to a folder
* Enter the folder and run the check (point to the place where you extracted the .jar)

```shell script
java -jar ../../apache-rat-0.13/apache-rat-0.13.jar -E .rat-excludes -d .
```

#### Verify the signatures

Make sure you have the key of person signed imported in your GPG. You can find the valid keys in
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS).

You can import the whole KEYS file:

```shell script
gpg --import KEYS
```

You can also import the keys individually from a keyserver. The below one uses Kaxil's key and
retrieves it from the default GPG keyserver
[OpenPGP.org](https://keys.openpgp.org):

```shell script
gpg --receive-keys 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
```

You should choose to import the key when asked.

Note that by being default, the OpenPGP server tends to be overloaded often and might respond with
errors or timeouts. Many of the release managers also uploaded their keys to the
[GNUPG.net](https://keys.gnupg.net) keyserver, and you can retrieve it from there.

```shell script
gpg --keyserver keys.gnupg.net --receive-keys 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
```

Once you have the keys, the signatures can be verified by running this:

```shell script
for i in *.asc
do
   echo "Checking $i"; gpg --verify `basename $i .sha512 `
done
```

This should produce results similar to the below. The "Good signature from ..." is indication
that the signatures are correct. Do not worry about the "not certified with a trusted signature"
warning. Most of the certificates used by release managers are self signed, that's why you get this
warning. By importing the server in the previous step and importing it via ID from
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS) page, you know that
this is a valid Key already.

```
Checking apache-airflow-1.10.12rc4-bin.tar.gz.asc
gpg: assuming signed data in 'apache-airflow-1.10.12rc4-bin.tar.gz'
gpg: Signature made sob, 22 sie 2020, 20:28:28 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
Checking apache_airflow-1.10.12rc4-py2.py3-none-any.whl.asc
gpg: assuming signed data in 'apache_airflow-1.10.12rc4-py2.py3-none-any.whl'
gpg: Signature made sob, 22 sie 2020, 20:28:31 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
Checking apache-airflow-1.10.12rc4-source.tar.gz.asc
gpg: assuming signed data in 'apache-airflow-1.10.12rc4-source.tar.gz'
gpg: Signature made sob, 22 sie 2020, 20:28:25 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
```

#### Verify the SHA512 sum

Run this:

```shell script
for i in *.sha512
do
    echo "Checking $i"; gpg --print-md SHA512 `basename $i .sha512 ` | diff - $i
done
```

You should get output similar to:

```
Checking apache-airflow-1.10.12rc4-bin.tar.gz.sha512
Checking apache_airflow-1.10.12rc4-py2.py3-none-any.whl.sha512
Checking apache-airflow-1.10.12rc4-source.tar.gz.sha512
```

### Verify if the Backport Packages release candidates "work" by Contributors

This can be done (and we encourage to) by any of the Contributors. In fact, it's best if the
actual users of Apache Airflow test it in their own staging/test installations. Each release candidate
is available on PyPI apart from SVN packages, so everyone should be able to install
the release candidate version of Airflow via simply (<VERSION> is 1.10.12 for example, and <X> is
release candidate number 1,2,3,....).

You can use any of the installation methods you prefer (you can even install it via the binary wheels
downloaded from the SVN).


#### Installing in your local virtualenv

You have to make sure you have Airilow 1.10.* installed in your PIP virtualenv
(the version you want to install providers with).

```shell script
pip install apache-airflow-backport-providers-<provider>==<VERSION>rc<X>
```

#### Installing with Breeze

There is also an easy way of installation with Breeze if you have the latest sources of Apache Airflow.
Here is a typical scenario.

First copy all the provider packages .whl files to the `dist` folder.

```shell script
./breeze start-airflow --install-airflow-version <VERSION>rc<X> \
    --python 3.7 --backend postgres --instal-wheels
```

For 1.10 releases you can also use `--no-rbac-ui` flag disable RBAC UI of Airflow:

```shell script
./breeze start-airflow --install-airflow-version <VERSION>rc<X> \
    --python 3.7 --backend postgres --install-wheels --no-rbac-ui
```

#### Building your own docker image

If you prefer to build your own image, you can also use the official image and PyPI packages to test
backport packages. This is especially helpful when you want to test integrations, but you need to install
additional tools. Below is an example Dockerfile, which installs backport providers for Google and
an additional third-party tools:

```dockerfile
FROM apache/airflow:1.10.12

RUN pip install --user apache-airflow-backport-providers-google==2020.10.5.rc1

RUN curl https://sdk.cloud.google.com | bash \
    && echo "source /home/airflow/google-cloud-sdk/path.bash.inc" >> /home/airflow/.bashrc \
    && echo "source /home/airflow/google-cloud-sdk/completion.bash.inc" >> /home/airflow/.bashrc

USER 0
RUN KUBECTL_VERSION="$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)" \
    && KUBECTL_URL="https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
    && curl -L "${KUBECTL_URL}" --output /usr/local/bin/kubectl \
    && chmod +x /usr/local/bin/kubectl

USER ${AIRFLOW_UID}
```

To build an image build and run a shell, run:

```shell script
docker build . -t my-airflow
docker run  -ti \
    --rm \
    -v "$PWD/data:/opt/airflow/" \
    -v "$PWD/keys/:/keys/" \
    -p 8080:8080 \
    -e GOOGLE_APPLICATION_CREDENTIALS=/keys/sa.json \
    -e AIRFLOW__CORE__LOAD_EXAMPLES=True \
    my-airflow bash
```

#### Verification

Once you install and run Airflow, you can perform any verification you see as necessary to check
that the Airflow works as you expected.

## Publish the final releases of backport packages

### Summarize the voting for the Backport Providers Release

Once the vote has been passed, you will need to send a result vote to dev@airflow.apache.org:

Subject:
```shell script
cat <<EOF
[RESULT][VOTE] Airflow Backport Providers ${VERSION}
EOF
```

Body:

```shell script
cat <<EOF

Hey all,

Airflow Backport Providers ${VERSION} (based on the ${VERSION_RC} candidate) has been accepted.

N "+1" binding votes received:
- PMC Member  (binding)
...

N "+1" non-binding votes received:

- COMMITER (non-binding)

Vote thread:
https://lists.apache.org/thread.html/<TODO:REPLACE_ME_WITH_THE_VOTING_THREAD>@%3Cdev.airflow.apache.org%3E

I'll continue with the release process and the release announcement will follow shortly.

Cheers,
<TODO: Your Name>

EOF

```

### Publish release to SVN

The best way of doing this is to svn cp  between the two repos (this avoids having to upload the binaries
again, and gives a clearer history in the svn commit logs.

We also need to archive older releases before copying the new ones
[Release policy](http://www.apache.org/legal/release-policy.html#when-to-archive)

```shell script
# Set the variables
export VERSION_RC=2020.5.20rc2
export VERSION=${VERSION_RC/rc?/}

# Set AIRFLOW_REPO_ROOT to the path of your git repo
export AIRFLOW_REPO_ROOT=$(pwd)

# Go to the directory where you have checked out the dev svn release
# And go to the sub-folder with RC candidates
cd "<ROOT_OF_YOUR_DEV_REPO>/backport-providers/${VERSION_RC}"
export SOURCE_DIR=$(pwd)

# Go the folder where you have checked out the release repo
# Clone it if it's not done yet
svn checkout https://dist.apache.org/repos/dist/release/airflow airflow-release

# Update to latest version
svn update

# Create backport-providers folder if it does not exist
# All latest releases are kept in this one folder without version sub-folder
mkdir -pv backport-providers
cd backport-providers

# Move the artifacts to svn folder & remove the rc postfix
for file in ${SOURCE_DIR}/*${VERSION_RC}*
do
  base_file=$(basename ${file})
  svn cp "${file}" "${base_file/${VERSION_RC}/${VERSION}}"
done


# If some packages have been excluded, remove them now
# Check the packages
ls *<provider>*
# Remove them
svn rm *<provider>*

# Check which old packages will be removed (you need python 3.6+)
python ${AIRFLOW_REPO_ROOT}/provider_packages/remove_old_releases.py \
    --directory .

# Remove those packages
python ${AIRFLOW_REPO_ROOT}/provider_packages/remove_old_releases.py \
    --directory . --execute


# Commit to SVN
svn commit -m "Release Airflow Backport Providers ${VERSION} from ${VERSION_RC}"
```

Verify that the packages appear in
[backport-providers](https://dist.apache.org/repos/dist/release/airflow/backport-providers)

### Publish the final version convenience package to PyPI

Checkout the RC Version:

```shell script
git checkout backport-providers-${VERSION_RC}
```

Tag and push the final version (providing that your apache remote is named 'apache'):

```shell script
git tag backport-providers-${VERSION}
git push apache backport-providers-${VERSION}
```

In order to publish to PyPI you just need to build and release packages.

* Generate the packages.

```shell script
./breeze prepare-provider-packages
```

if you ony build few packages, run:

```shell script
./breeze prepare-provider-packages
```

In case you decided to remove some of the packages. Remove them from dist folder now:

```shell script
ls dist/*<provider>*
rm dist/*<provider>*
```


* Verify the artifacts that would be uploaded:

```shell script
twine check dist/*
```

* Upload the package to PyPi's test environment:

```shell script
twine upload -r pypitest dist/*
```

* Verify that the test packages look good by downloading it and installing them into a virtual environment.
Twine prints the package links as output - separately for each package.

* Upload the package to PyPi's production environment:

```shell script
twine upload -r pypi dist/*
```

### Notify developers of release

- Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org and announce@apache.org) that
the artifacts have been published:

### Notify developers of release

- Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org and announce@apache.org) that
the artifacts have been published:

Subject:
```shell script
cat <<EOF
Airflow Backport Providers ${VERSION} are released
EOF
```

Body:
```shell script
cat <<EOF
Dear Airflow community,

I'm happy to announce that Airflow Backport Providers packages ${VERSION} were just released.

The source release, as well as the binary releases, are available here:

https://dist.apache.org/repos/dist/release/airflow/backport-providers/

We also made those versions available on PyPi for convenience ('pip install apache-airflow-backport-providers-*'):

https://pypi.org/search/?q=apache-airflow-backport-providers

The documentation and changelogs are available in the PyPI packages:

<PASTE TWINE UPLOAD LINKS HERE>


Cheers,
<your name>
EOF
```


### Update Announcements page

Update "Announcements" page at the [Official Airflow website](https://airflow.apache.org/announcements/)

----------------------------------------------------------------------------------------------------------------------

## Prepare the Regular Provider Packages Alpha

### Generate release notes

Prepare release notes for all the packages you plan to release. Note that for now version number is
hard-coded to 0.0.1 for all packages. Later on we are going to update the versions according
to SEMVER versioning.

Details about maintaining the SEMVER version are going to be discussed and implemented in
[the related issue](https://github.com/apache/airflow/issues/11425)


```shell script
./breeze prepare-provider-readme [packages]
```

You can iterate and re-generate the same readme content as many times as you want.
Generated readme files should be eventually committed to the repository.

### Build an Alpha release for SVN apache upload

The Alpha artifacts we vote upon should be the exact ones in the future we vote against, without any
modification than renaming i.e. the contents of the files must be the same between voted
release candidate and final release. Because of this the version in the built artifacts
that will become the official Apache releases must not include the rcN suffix. They also need
to be signed and have checksum files. You can generate the checksum/signature files by running
the "dev/sign.sh" script (assuming you have the right PGP key set-up for signing). The script
generates corresponding .asc and .sha512 files for each file to sign.

#### Build and sign the source and convenience packages

Currently, we are releasing alpha provider packages together with the main sources of Airflow. In the future
we are going to add procedure to release the sources of released provider packages separately.
Details are in [the related issue](https://github.com/apache/airflow/issues/11425)

* Generate the packages - since we are preparing packages for SVN repo, we should use the right switch. Note
  that this will clean up dist folder before generating the packages, so it will only contain the packages
  you intended to build.

```shell script
export VERSION=0.0.1alpha1

./breeze prepare-provider-packages --version-suffix-for-svn alpha1
```

if you ony build few packages, run:

```shell script
./breeze prepare-provider-packages --version-suffix-for-svn alpha1 PACKAGE PACKAGE ....
```

* Sign all your packages

```shell script
pushd dist
../dev/sign.sh *
popd
```

#### Commit the source packages to Apache SVN repo

* Push the artifacts to ASF dev dist repo

```shell script
# First clone the repo if you do not have it
svn checkout https://dist.apache.org/repos/dist/dev/airflow airflow-dev

# update the repo in case you have it already
cd airflow-dev
svn update

# Create a new folder for the release.
cd airflow-dev/providers
svn mkdir ${VERSION}

# Move the artifacts to svn folder
mv ${AIRFLOW_REPO_ROOT}/dist/* ${VERSION}/

# Add and commit
svn add ${VERSION}/*
svn commit -m "Add artifacts for Airflow Providers ${VERSION}"

cd ${AIRFLOW_REPO_ROOT}
```

Verify that the files are available at
[backport-providers](https://dist.apache.org/repos/dist/dev/airflow/backport-providers/)

### Publish the Alpha convenience package to PyPI

In order to publish to PyPI you just need to build and release packages. The packages should however
contain the rcN suffix in the version name as well, so you need to use `--version-suffix-for-pypi` switch
to prepare those packages. Note that these are different packages than the ones used for SVN upload
though they should be generated from the same sources.

* Generate the packages with the right RC version (specify the version suffix with PyPI switch). Note that
this will clean up dist folder before generating the packages, so you will only have the right packages there.

```shell script
./breeze prepare-provider-packages --version-suffix-for-pypi alpha1
```

if you ony build few packages, run:

```shell script
./breeze prepare-provider-packages --version-suffix-for-pypi alpha1 PACKAGE PACKAGE ....
```

* Verify the artifacts that would be uploaded:

```shell script
twine check dist/*
```

* Upload the package to PyPi's test environment:

```shell script
twine upload -r pypitest dist/*
```

* Verify that the test packages look good by downloading it and installing them into a virtual environment.
Twine prints the package links as output - separately for each package.

* Upload the package to PyPi's production environment:

```shell script
twine upload -r pypi dist/*
```

* Copy the list of links to the uploaded packages - they will be useful in preparing VOTE email.

* Again, confirm that the packages are available under the links printed.
