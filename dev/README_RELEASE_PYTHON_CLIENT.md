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

- [Release Process](#release-process)
  - [Prepare PyPI convenience "RC" packages](#prepare-pypi-convenience-rc-packages)
  - [Prepare Vote email on the Airflow Client release candidate](#prepare-vote-email-on-the-airflow-client-release-candidate)
- [Verify the release candidate by PMC members](#verify-the-release-candidate-by-pmc-members)
  - [SVN check](#svn-check)
  - [Reproducible package check](#reproducible-package-check)
  - [Signature check](#signature-check)
  - [SHA512 checksum check](#sha512-checksum-check)
- [Verify the release candidate by Contributors](#verify-the-release-candidate-by-contributors)
  - [Testing with Breeze's start-airflow](#testing-with-breezes-start-airflow)
- [Publish the final Apache Airflow client release](#publish-the-final-apache-airflow-client-release)
  - [Summarize the voting for the Apache Airflow client release](#summarize-the-voting-for-the-apache-airflow-client-release)
  - [Publish release to SVN](#publish-release-to-svn)
  - [Prepare PyPI "release" packages](#prepare-pypi-release-packages)
  - [Create release on GitHub](#create-release-on-github)
  - [Notify developers of release](#notify-developers-of-release)
  - [Add release data to Apache Committee Report Helper](#add-release-data-to-apache-committee-report-helper)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Release Process

The client versioning is independent of the Airflow versioning.

The Python client is generated using Airflow's [openapi spec](https://github.com/apache/airflow/blob/master/clients/gen/python.sh).
To update the client for new APIs do the following steps:

```bash
- Checkout the v2-*-test branch of Airflow where you generate the client from

```bash
# If you have not done so yet
git clone git@github.com/apache/airflow
cd airflow
# Checkout the right branch
git checkout v2-8-test
export AIRFLOW_REPO_ROOT=$(pwd -P)
cd ..
```

- Checkout the right branch (usually main) of the Airflow Python client where you
  generate the source code to

```bash
# If you have not done so yet
git clone git@github.com:apache/airflow-client-python
cd airflow-client-python
# Checkout the right branch
git checkout main
export CLIENT_REPO_ROOT=$(pwd -P)
cd ..
```

- Set your version in `clients/python/version.txt` in the **Airflow** repository.
  Set the `VERSION` and `VERSION_SUFFIX` environment variables. Note that version.txt should contain
  the target version - without the suffix.

```bash
cd ${AIRFLOW_REPO_ROOT}
export VERSION="2.8.0"
export VERSION_SUFFIX="rc1"
echo "${VERSION}" > clients/python/version.txt
```

- Get a diff between the last airflow version and the current airflow version
  which this release is based on:

```shell script
cd ${AIRFLOW_REPO_ROOT}
git log 2.8.0..HEAD --pretty=oneline -- airflow/api_connexion/openapi/v1.yaml
```

- Update CHANGELOG.md with the details.

- Create PR where you add the changelog in `main` branch and cherry-pick it to the `v2-test` branch - same
  as in case of Airflow changelog.

- Merge it to the `v2-*-stable` branch. You will release API client from the latest `v2-*-stable` branch
  of Airflow repository - same branch that is used to release Airflow.

- Build the sdist and wheel packages to be added to SVN and copy generated client sources to the
  Python Client repository.

```shell script
cd ${AIRFLOW_REPO_ROOT}
rm dist/*
breeze release-management prepare-python-client --package-format both --python-client-repo "${CLIENT_REPO_ROOT}"
```

- This should generate both sdist and .whl package in `dist` folder of the Airflow repository. It should
  also override the client sources in the Python Client repository.

- Commit generated code to the main branch of the Python Client repository. No need to have code reviews
  for that code if it is automatically generated. However make sure that `git diff HEAD` shows expected
  changes only.

```shell script
cd ${CLIENT_REPO_ROOT}
git diff HEAD
git checkout -b release-${VERSION}
git add .
git commit -m "Update Python Client to ${VERSION}${VERSION_SUFFIX}"
git push apache release-${VERSION}
```

Then open a PR and merge it into main.

- Tag your release with RC candidate tag (note that this is the RC tag even if version of the packages
  is the same as the final version. This is because when the packages get approved and released they
  will turn into official release and must be binary identical to the RC packages in SVN). The tags
  should be set in both Airflow and Airflow Client repositories (with python-client prefix in Airflow repo and
  without the prefix in the Python Client repo).

```shell script
cd ${AIRFLOW_REPO_ROOT}
git tag -s python-client-${VERSION}${VERSION_SUFFIX} -m "Airflow Python Client ${VERSION}${VERSION_SUFFIX}"
git push apache python-client-${VERSION}${VERSION_SUFFIX}
cd ${CLIENT_REPO_ROOT}
git tag -s ${VERSION}${VERSION_SUFFIX} -m "Airflow Python Client ${VERSION}${VERSION_SUFFIX}"
git push apache tag ${VERSION}${VERSION_SUFFIX}
```


- Generate signatures and checksum files for the packages (if you have not generated a key yet, generate
  it by following instructions on http://www.apache.org/dev/openpgp.html#key-gen-generate-key)

```shell script
cd ${AIRFLOW_REPO_ROOT}
pushd dist
../dev/sign.sh *
popd
```

- Commit the artifacts to ASF dev dist SVN repository

```shell script
# First clone the repo somewhere if you have not done it yet
svn checkout https://dist.apache.org/repos/dist/dev/airflow airflow-dev

# Create new folder for the release
cd airflow-dev/clients/python
svn mkdir ${VERSION}${VERSION_SUFFIX}

# Move the artifacts to svn folder & commit
mv ${AIRFLOW_REPO_ROOT}/dist/apache_airflow_client-* ${VERSION}${VERSION_SUFFIX}/
cd ${VERSION}${VERSION_SUFFIX}
svn add *
svn commit -m "Add artifacts for Apache Airflow Python Client ${VERSION}${VERSION_SUFFIX}"

# Remove old version
cd ..
export PREVIOUS_VERSION_WITH_SUFFIX=2.8.0rc1
svn rm ${PREVIOUS_VERSION_WITH_SUFFIX}
svn commit -m "Remove old Apache Airflow Python Client ${PREVIOUS_VERSION_WITH_SUFFIX}"
```

## Prepare PyPI convenience "RC" packages

At this point we have the artefact that we vote on, but as a convenience to developers we also want to
publish "snapshots" of the RC builds to pypi for installing via pip. Note that these packages must be
generated with RC version.

To do this we need to:

- Build the package with the RC version. Note that we are not copying the generated sources, we just
  build the package from the sources in the Airflow repository and generate packages from those sources.

```shell script
rm dist/*
breeze release-management prepare-python-client --package-format both --version-suffix-for-pypi "${VERSION_SUFFIX}"
```

- Verify the artifacts that would be uploaded:

```shell script
twine check dist/*
```

- Upload the package to PyPi's production environment:

```shell script
twine upload -r pypi dist/*
```

- Confirm that the package is available here: https://pypi.python.org/pypi/apache-airflow-client

## Prepare Vote email on the Airflow Client release candidate

Subject:

```shell script
cat <<EOF
[VOTE] Release Apache Airflow Python Client ${VERSION} from ${VERSION}${VERSION_SUFFIX}
EOF
```

Body:

```shell script
cat <<EOF
Hey fellow Airflowers,

I have cut the first release candidate for the Apache Airflow Python Client ${VERSION}.
This email is calling for a vote on the release,
which will last for 72 hours. Consider this my (binding) +1.

Airflow Client ${VERSION}${VERSION_SUFFIX} is available at:
https://dist.apache.org/repos/dist/dev/airflow/clients/python/${VERSION}${VERSION_SUFFIX}/

The apache_airflow_client-${VERSION}.tar.gz is an sdist release that contains INSTALL instructions, and also
is the official source release.

The apache_airflow_client-${VERSION}-py3-none-any.whl is a binary wheel release that pip can install.

Those packages do not contain .rc* version as, when approved, they will be released as the final version.

The rc packages are also available at PyPI (with rc suffix) and you can install it with pip as usual:
https://pypi.org/project/apache-airflow-client/${VERSION}${VERSION_SUFFIX}/

Public keys are available at:
https://dist.apache.org/repos/dist/release/airflow/KEYS

Only votes from PMC members are binding, but all members of the community
are encouraged to test the release and vote with "(non-binding)".

The test procedure for PMC members is described in:
https://github.com/apache/airflow/blob/main/dev/README_RELEASE_PYTHON_CLIENT.md#verify-the-release-candidate-by-pmc-members

The test procedure for contributors and members of the community who would like to test this RC is described in:
https://github.com/apache/airflow/blob/main/dev/README_RELEASE_PYTHON_CLIENT.md#verify-the-release-candidate-by-contributors

*Changelog:*

*Major changes:*
...

*Major fixes:*
...

*New API supported:*
...

Cheers,
<your name>
EOF
```

# Verify the release candidate by PMC members

PMC members should verify the releases in order to make sure the release is following the
[Apache Legal Release Policy](http://www.apache.org/legal/release-policy.html).

At least 3 (+1) votes should be recorded in accordance to
[Votes on Package Releases](https://www.apache.org/foundation/voting.html#ReleaseVotes)

The legal checks include:

* verifying if packages can be reproducibly built from sources
* checking if the packages are present in the right dist folder on svn
* verifying if release manager signed the releases with the right key
* verifying if all the checksums are valid for the release
* verifying if all the sources have correct licences

## SVN check

The files should be present in the sub-folder of
[Airflow dist](https://dist.apache.org/repos/dist/dev/airflow/clients/python)

The following files should be present (6 files):

* .tar.gz + .asc + .sha512
* -py3-none-any.whl + .asc + .sha512

As a PMC member, you should be able to clone the SVN repository

```shell script
svn co https://dist.apache.org/repos/dist/dev/airflow/clients/python
```

Or update it if you already checked it out:

```shell script
svn update .
```

## Reproducible package check

Airflow Python client supports reproducible builds, which means that the packages prepared from the same
sources should produce binary identical packages in reproducible way. You should check if the packages can be
binary-reproduced when built from the sources.

Checkout airflow sources and build packages in dist folder (replace X.Y.Zrc1 with the version + rc candidate)
you are checking):

```shell script
VERSION=X.Y.Zrc1
git checkout python-client-${VERSION}
export AIRFLOW_REPO_ROOT=$(pwd)
rm -rf dist/*
breeze release-management prepare-python-client --package-format both
```

The last - build step - by default will use Dockerized build and building of Python client packages
will be done in a docker container.  However, if you have  `hatch` installed locally you can use
`--use-local-hatch` flag and it will build and use  docker image that has `hatch` installed.

```bash
breeze release-management prepare-python-client --package-format both --use-local-hatch
```

This is generally faster and requires less resources/network bandwidth.

Both commands should produce reproducible `.whl`, `.tar.gz` packages in dist folder.

Change to the directory where you have the packages from svn:

```shell script
# First clone the repo if you do not have it
cd ..
[ -d asf-dist ] || svn checkout --depth=immediates https://dist.apache.org/repos/dist asf-dist
svn update --set-depth=infinity asf-dist/dev/airflow/clients/python

# Then compare the packages
cd asf-dist/dev/airflow/clients/python/${VERSION}
for i in ${AIRFLOW_REPO_ROOT}/dist/*
do
  echo "Checking if $(basename $i) is the same as $i"
  diff "$(basename $i)" "$i" && echo "OK"
done
```

The output should be empty (files are identical).
In case the files are different, you should see:

```
Binary files apache_airflow-client-2.9.0.tar.gz and .../apache_airflow-2.9.0.tar.gz differ
```

## Signature check

Make sure you have imported into your GPG the PGP key of the person signing the release. You can find the valid keys in
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS).

You can import the whole KEYS file:

```shell script
gpg --import KEYS
```

You can also import the keys individually from a keyserver. The below one uses Kaxil's key and
retrieves it from the default GPG keyserver
[OpenPGP.org](https://keys.openpgp.org):

```shell script
gpg --keyserver keys.openpgp.org --receive-keys CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
```

You should choose to import the key when asked.

Note that by being default, the OpenPGP server tends to be overloaded often and might respond with
errors or timeouts. Many of the release managers also uploaded their keys to the
[GNUPG.net](https://keys.gnupg.net) keyserver, and you can retrieve it from there.

```shell script
gpg --keyserver keys.gnupg.net --receive-keys CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
```

Once you have the keys, the signatures can be verified by running this:

```shell script
for i in *.asc
do
   echo -e "Checking $i\n"; gpg --verify $i
done
```

This should produce results similar to the below. The "Good signature from ..." is indication
that the signatures are correct. Do not worry about the "not certified with a trusted signature"
warning. Most of the certificates used by release managers are self-signed, and that's why you get this
warning. By importing the key either from the server in the previous step or from the
[KEYS](https://dist.apache.org/repos/dist/release/airflow/KEYS) page, you know that
this is a valid key already.  To suppress the warning you may edit the key's trust level
by running `gpg --edit-key <key id> trust` and entering `5` to assign trust level `ultimate`.

```
Checking apache-airflow-2.0.2rc4.tar.gz.asc
gpg: assuming signed data in 'apache-airflow-2.0.2rc4.tar.gz'
gpg: Signature made sob, 22 sie 2020, 20:28:28 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B

Checking apache_airflow-2.0.2rc4-py2.py3-none-any.whl.asc
gpg: assuming signed data in 'apache_airflow-2.0.2rc4-py2.py3-none-any.whl'
gpg: Signature made sob, 22 sie 2020, 20:28:31 CEST
gpg:                using RSA key 12717556040EEF2EEAF1B9C275FCCD0A25FA0E4B
gpg: Good signature from "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: This key is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: 1271 7556 040E EF2E EAF1  B9C2 75FC CD0A 25FA 0E4B
```

## SHA512 checksum check

Run this:

```shell script
for i in *.sha512
do
    echo "Checking $i"; shasum -a 512 `basename $i .sha512 ` | diff - $i
done
```

You should get output similar to:

```
Checking apache-airflow-client-2.0.2rc4.tar.gz.sha512
Checking apache_airflow-client-2.0.2rc4-py2.py3-none-any.whl.sha512
```


# Verify the release candidate by Contributors

This can be done (and we encourage to) by any of the Contributors. In fact, it's best if the
actual users of Airflow Client test it in their own staging/test installations. Each release candidate
is available on PyPI apart from SVN packages, so everyone should be able to install
the release candidate version of Airflow Client via simply (<VERSION> is 2.0.0 for example, and <X> is
release candidate number 1,2,3,....).

Once you install and run Airflow Client, you should perform any verification you see as necessary to check
that the client works as you expected.

## Testing with Breeze's start-airflow

You can test the client by running the `start-airflow` command from Breeze. This will start Airflow
and allows you to test the client in a real environment.

1. Enable basic authentication configuration for breeze. This can be done by setting the right environment
   variable in `files/airflow-breeze-config/init.sh`:

```shell
export AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.session,airflow.api.auth.backend.basic_auth
export AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
```


When you enter Breeze the webserver will be available at `http://localhost:8080` (from inside the container)
or `http://localhost:28080` from the host) and you should be able to access the API
with `admin`/`admin` credentials. The `http://localhost:8080` and `admin`/`admin` credentials are
default in the `clients/python/test_python_client.py` test.

The ``AIRFLOW__WEBSERVER__EXPOSE_CONFIG`` is optional - the script will also succeed when
(default setting) exposing configuration is disabled.

2. Start Airflow in Breeze with example dags enabled:

```shell
breeze start-airflow --load-example-dags
```

Give the server 20-30 seconds to serialize the example Dags to DB

3. In the meantime install the python client you want to test - in the terminal window in the container.
   It can be installed from `PyPI` via `pip install apache-airflow-client==X.Y.Zrc1` or installed
   from a file (for example if you nust built the client with `breeze release-management prepare-python-client`,
   there will be `/dist/apache_airflow_client-py*.whl` file in in your `/dist` folder (mapped from `dist`
   folder in your checked out airflow repository and you can install it with:
   `pip install /dist/apache_airflow_client-py*.whl`.

4. The client script is available in `/opt/airflow/clients/python/test_python_client.py` and you can run it
   from inside the container with:

```shell script
python /opt/airflow/clients/python/test_python_client.py
```


# Publish the final Apache Airflow client release

## Summarize the voting for the Apache Airflow client release

```shell script
Hello,

Apache Airflow Python Client 2.5.0 (based on RC1) has been accepted.

3 "+1" binding votes received:
- Ephraim Anierobi
- Jarek Potiuk
- Jed Cunningham


1 "+1" non-binding votes received:

- Pierre Jeambrun

Vote thread:
https://lists.apache.org/thread/1qcj0r67dff3zg0w2vyfhr30fx9xtp3y

I'll continue with the release process, and the release announcement will follow shortly.

Cheers,
<your name>
```

## Publish release to SVN

```shell script
# Go to Airflow sources first
cd <YOUR_AIRFLOW_REPO_ROOT>
export AIRFLOW_REPO_ROOT="$(pwd)"
# Go to Airflow python client sources first
cd <YOUR_AIRFLOW_CLIENT_REPO_ROOT>
export CLIENT_REPO_ROOT="$(pwd)"
cd ..
# Clone the AS
[ -d asf-dist ] || svn checkout --depth=immediates https://dist.apache.org/repos/dist asf-dist
svn update --set-depth=infinity asf-dist/{release,dev}/airflow
CLIENT_DEV_SVN="${PWD}/asf-dist/dev/airflow/clients/python"
CLIENT_RELEASE_SVN="${PWD}/asf-dist/release/airflow/clients/python"
cd "${CLIENT_RELEASE_SVN}"

export VERSION="2.8.1"
# Update the approved RC version here
export VERSION_SUFFIX="rc1"
# Update the previous version that have been released here
# There should be only one version in https://downloads.apache.org/airflow/clients/python/
# Policy here: http://www.apache.org/legal/release-policy.html#when-to-archive
export PREVIOUS_VERSION=2.8.0

# Create new folder for the release
svn mkdir ${VERSION}
cd ${VERSION}

# Move the artifacts to svn folder & commit
for f in ${CLIENT_DEV_SVN}/${VERSION}${VERSION_SUFFIX}/*; do
  svn cp $f . ;
done
# Remove old release
cd ..
svn rm ${PREVIOUS_VERSION}
svn commit -m "Release Apache Airflow Python Client ${VERSION} from ${VERSION}${VERSION_SUFFIX}"
```

Verify that the packages appear in [airflow](https://dist.apache.org/repos/dist/release/airflow/clients/python)

## Prepare PyPI "release" packages

We need to upload the packages to PyPI. Note that we are not copying the generated sources, we just

```shell script
cd ${VERSION}
twine check *.tar.gz *.whl
```

- Upload the package to PyPi's production environment:

```shell script
twine upload -r pypi *.tar.gz *.whl
```

- Confirm that the package is available here: https://pypi.python.org/pypi/apache-airflow-client

- Push Tag for the final version to the Airflow repository and the Python Client repository:

```shell script
cd ${AIRFLOW_REPO_ROOT}
git checkout python-client-${VERSION}${VERSION_SUFFIX}
git tag -s python-client-${VERSION} -m "Airflow Python Client ${VERSION}"
git push apache tag python-client-${VERSION}
cd ${CLIENT_REPO_ROOT}
git checkout ${VERSION}${VERSION_SUFFIX}
git tag -s ${VERSION} -m ${VERSION}
git push origin tag ${VERSION}
```

## Create release on GitHub

Create a new release on GitHub "airflow-python-client" repo  with the release notes and assets
from the release svn.

## Notify developers of release

Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org) that the artifacts have been published:

Subject:

```
cat <<EOF
[ANNOUNCE] Apache Airflow Python Client ${VERSION} Released
EOF
```

Body:

```
cat <<EOF
Dear Airflow community,

I'm happy to announce that Apache Airflow Python Client ${VERSION} was just released.

We made this version available on PyPI for convenience:
\`pip install apache-airflow-client\`
https://pypi.org/project/apache-airflow-client/${VERSION}/

The documentation is available at:
https://github.com/apache/airflow-client-python/

Find the changelog here for more details:
https://github.com/apache/airflow-client-python/blob/main/CHANGELOG.md

Thanks,
<your name>
EOF
```

## Add release data to Apache Committee Report Helper

Add the release data (version and date) at: https://reporter.apache.org/addrelease.html?airflow
