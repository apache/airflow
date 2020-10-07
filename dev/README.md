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

- [Development Tools](#development-tools)
  - [Airflow release signing tool](#airflow-release-signing-tool)
- [Verifying the release candidate by PMCs (legal)](#verifying-the-release-candidate-by-pmcs-legal)
  - [PMC voting](#pmc-voting)
  - [SVN check](#svn-check)
  - [Verifying the licences](#verifying-the-licences)
  - [Verifying the signatures](#verifying-the-signatures)
  - [Verifying the SHA512 sum](#verifying-the-sha512-sum)
- [Verifying if the release candidate "works" by Contributors](#verifying-if-the-release-candidate-works-by-contributors)
- [Building an RC](#building-an-rc)
- [PyPI Snapshots](#pypi-snapshots)
- [Make sure your public key is on id.apache.org and in KEYS](#make-sure-your-public-key-is-on-idapacheorg-and-in-keys)
- [Voting on an RC](#voting-on-an-rc)
- [Publishing release](#publishing-release)
- [Publishing to PyPi](#publishing-to-pypi)
- [Updating CHANGELOG.md](#updating-changelogmd)
- [Notifying developers of release](#notifying-developers-of-release)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Development Tools

## Airflow release signing tool

The release signing tool can be used to create the SHA512/MD5 and ASC files that required for Apache releases.

### Execution

To create a release tarball execute following command from Airflow's root.

```bash
python setup.py compile_assets sdist --formats=gztar
```

*Note: `compile_assets` command build the frontend assets (JS and CSS) files for the
Web UI using webpack and yarn. Please make sure you have `yarn` installed on your local machine globally.
Details on how to install `yarn` can be found in CONTRIBUTING.rst file.*

After that navigate to relative directory i.e., `cd dist` and sign the release files.

```bash
../dev/sign.sh <the_created_tar_ball.tar.gz
```

Signing files will be created in the same directory.


# Verifying the release candidate by PMCs (legal)

## PMC voting

The PMCs should verify the releases in order to make sure the release is following the
[Apache Legal Release Policy](http://www.apache.org/legal/release-policy.html).

At least 3 (+1) votes should be recorded in accordance to
[Votes on Package Releases](https://www.apache.org/foundation/voting.html#ReleaseVotes)

The legal checks include:

* checking if the packages are present in the right dist folder on svn
* verifying if all the sources have correct licences
* verifying if release manager signed the releases with the right key
* verifying if all the checksums are valid for the release

## SVN check

The files should be present in the sub-folder of
[Airflow dist](https://dist.apache.org/repos/dist/dev/airflow/)

The following files should be present (9 files):

* -bin-tar.gz + .asc + .sha512
* -source.tar.gz + .asc + .sha512
* -.whl + .asc + .sha512

As a PMC you should be able to clone the SVN repository:

```bash
svn co https://dist.apache.org/repos/dist/dev/airflow
```

Or update it if you already checked it out:

```bash
svn update .
```

## Verifying the licences

This can be done with the Apache RAT tool.

* Download the latest jar from https://creadur.apache.org/rat/download_rat.cgi (unpack the sources,
  the jar is inside)
* Unpack the -source.tar.gz to a folder
* Enter the folder and run the check (point to the place where you extracted the .jar)

```bash
java -jar ../../apache-rat-0.13/apache-rat-0.13.jar -E .rat-excludes -d .
```

## Verifying the signatures

Make sure you have the key of person signed imported in your GPG. You can find the valid keys in
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS).

You can import the whole KEYS file:

```bash
gpg --import KEYS
```

You can also import the keys individually from a keyserver. The below one uses Kaxil's key and
retrieves it from the default GPG keyserver
[OpenPGP.org](https://keys.openpgp.org):

```bash
gpg --receive-keys 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
```

You should choose to import the key when asked.

Note that by being default, the OpenPGP server tends to be overloaded often and might respond with
errors or timeouts. Many of the release managers also uploaded their keys to the
[GNUPG.net](https://keys.gnupg.net) keyserver, and you can retrieve it from there.

```bash
gpg --keyserver keys.gnupg.net --receive-keys 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
```

Once you have the keys, the signatures can be verified by running this:

```bash
for i in *.asc
do
   echo "Checking $i"; gpg --verify `basename $i .sha512 `
done
```

This should produce results similar to the below. The "Good signature from ..." is indication
that the signatures are correct. Do not worry about the "not certified with a trusted signature"
warning. Most of certificates used by release managers are self signed, that's why you get this
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

## Verifying the SHA512 sum

Run this:

```bash
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

# Verifying if the release candidate "works" by Contributors

This can be done (and we encourage to) by any of the Contributors. In fact, it's best if the
actual users of Apache Airflow test it in their own staging/test installations. Each release candidate
is available on PyPI apart from SVN packages, so everyone should be able to install
the release candidate version of Airflow via simply (<VERSION> is 1.10.12 for example, and <X> is
release candidate number 1,2,3,....).

```bash
pip install apache-airflow==<VERSION>rc<X>
```
Optionally it can be followed with constraints

```bash
pip install apache-airflow==<VERSION>rc<X> \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-<VERSION>/constraints-3.6.txt"`
```

Note that the constraints contain python version that you are installing it with.

You can use any of the installation methods you prefer (you can even install it via the binary wheel
downloaded from the SVN).

There is also an easy way of installation with Breeze if you have the latest sources of Apache Airflow.
Here is a typical scenario:

1. `./breeze --install-airflow-version <VERSION>rc<X> --python 3.7 --backend postgres`
2. `tmux`
3. Hit Ctrl-B followed by "
4. `airflow resetdb -y`
5. if you want RBAC:
     * Change RBAC setting: `sed "s/rbac = False/rbac = True/" -i /root/airflow/airflow.cfg`
     * airflow resetdb -y
     * Run`airflow create_user  -r Admin -u airflow -e airflow@apache.org -f Airflow -l User -p airflow
6. `airflow scheduler`
7. Ctrl-B "up-arrow"
8. `airflow webserver`

Once you install and run Airflow, you should perform any verification you see as necessary to check
that the Airflow works as you expected.


# Building an RC

The Release Candidate artifacts we vote upon should be the exact ones we vote against, without any modification than renaming – i.e. the contents of the files must be the same between voted release canidate and final release. Because of this the version in the built artifacts that will become the official Apache releases must not include the rcN suffix.

- Set environment variables
```
# Set Version
export VERSION=1.10.2rc3


# Set AIRFLOW_REPO_ROOT to the path of your git repo
export AIRFLOW_REPO_ROOT=$(pwd)


# Example after cloning
git clone https://github.com/apache/airflow.git airflow
cd airflow
export AIRFLOW_REPO_ROOT=$(pwd)
```

- set your version to 1.10.2 in airflow/version.py (without the RC tag)
- Commit the version change.

- Tag your release

`git tag ${VERSION}`

- Clean the checkout: the sdist step below will
`git clean -fxd`

- Tarball the repo
`git archive --format=tar.gz ${VERSION} --prefix=apache-airflow-${VERSION}/ -o apache-airflow-${VERSION}-source.tar.gz`

- Generate sdist
NOTE: Make sure your checkout is clean at this stage - any untracked or changed files will otherwise be included in the file produced.
`python setup.py compile_assets sdist bdist_wheel`

- Rename the sdist
```
mv dist/apache-airflow-${VERSION%rc?}.tar.gz apache-airflow-${VERSION}-bin.tar.gz
mv dist/apache_airflow-${VERSION%rc?}-py2.py3-none-any.whl apache_airflow-${VERSION}-py2.py3-none-any.whl
```

- Generate SHA512/ASC (If you have not generated a key yet, generate it by following instructions on http://www.apache.org/dev/openpgp.html#key-gen-generate-key)
```
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

# PyPI Snapshots

At this point we have the artefact that we vote on, but as a convenience to developers we also want to publish "snapshots" of the RC builds to pypi for installing via pip. To do this we need to

- Edit the airflow/version.py to include the RC suffix.
- python setup.py compile_assets sdist bdist_wheel
- Follow the steps in [here](#Publishing-to-PyPi)
- Throw away the change - we don't want to commit this: `git checkout airflow/version.py`

It is important to stress that this snapshot is not intended for users.

# Make sure your public key is on id.apache.org and in KEYS

You will need to sign the release artifacts with your pgp key. After you have created a key, make sure you

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

# Voting on an RC

- Once the RC is built (both source and binary), put them in the dev SVN repository:
https://dist.apache.org/repos/dist/dev/airflow/

- Use the dev/airflow-jira script to generate a list of Airflow JIRAs that were closed in the release.

- Send out a vote to the dev@airflow.apache.org mailing list:

<details><summary>[VOTE] Airflow 1.10.2rc3</summary>
<p>

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

Only votes from PMC members are binding, but members of the community are
encouraged to test the release and vote with "(non-binding)".

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
</p>
</details>

- Once the vote has been passed, you will need to send a result vote to dev@airflow.apache.org:

<details><summary>[RESULT][VOTE] Airflow 1.10.2rc3</summary>
<p>
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

I'll continue with the release process and the release announcement will follow shortly.

Cheers,
<your name>

</p>
</details>

# Publishing release

- After both votes pass (see above), you need to migrate the RC artifacts that passed to this repository:
https://dist.apache.org/repos/dist/release/airflow/
(The migration should including renaming the files so that they no longer have the RC number in their filenames.)

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

# Publishing to PyPi

- Create a ~/.pypirc file:

```shell script
$ cat ~/.pypirc
[distutils]
index-servers =
pypi
pypitest

[pypi]
username=your-username
password=*********

[pypitest]
repository=https://test.pypi.org/legacy/
username=your-username
password=*********
```

(more details [here](https://peterdowns.com/posts/first-time-with-pypi.html).)

- Set proper permissions for the pypirc file:
`$ chmod 600 ~/.pypirc`

- Confirm that airflow/version.py is set properly.

- Install [twine](https://pypi.org/project/twine/) if you do not have it already.
`pip install twine`

- Build the package:
`python setup.py compile_assets sdist bdist_wheel`

- Verify the artifacts that would be uploaded:
`twine check dist/*`

- Upload the package to PyPi's test environment:
`twine upload -r pypitest dist/*`

- Verify that the test package looks good by downloading it and installing it into a virtual environment. The package download link is available at:
https://test.pypi.org/project/apache-airflow/#files

- Upload the package to PyPi's production environment:
`twine upload -r pypi dist/*`

- Again, confirm that the package is available here:
https://pypi.python.org/pypi/apache-airflow

# Updating CHANGELOG.md

- Get a diff between the last version and the current version:
`$ git log 1.8.0..1.9.0 --pretty=oneline`
- Update CHANGELOG.md with the details, and commit it.

# Notifying developers of release

- Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org and announce@apache.org) that the artifacts have been published:

<details><summary>Airflow 1.10.2 is released</summary>
<p>
Dear Airflow community,

I'm happy to announce that Airflow 1.10.2 was just released.

The source release, as well as the binary "sdist" release, are available
here:

https://dist.apache.org/repos/dist/release/airflow/1.10.2/

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
</p>
</details>

- Update the Announcement page on
[Airflow Site](https://airflow.apache.org/announcements/)
