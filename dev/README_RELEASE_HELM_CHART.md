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

- [Prepare the Apache Airflow Helm Chart Release Candidate](#prepare-the-apache-airflow-helm-chart-release-candidate)
  - [Pre-requisites](#pre-requisites)
  - [Build Changelog](#build-changelog)
  - [Build RC artifacts](#build-rc-artifacts)
  - [Prepare Vote email on the Apache Airflow release candidate](#prepare-vote-email-on-the-apache-airflow-release-candidate)
- [Verify the release candidate by PMCs](#verify-the-release-candidate-by-pmcs)
  - [SVN check](#svn-check)
  - [Licence check](#licence-check)
  - [Signature check](#signature-check)
  - [SHA512 sum check](#sha512-sum-check)
- [Verify release candidates by Contributors](#verify-release-candidates-by-contributors)
- [Publish the final release](#publish-the-final-release)
  - [Summarize the voting for the release](#summarize-the-voting-for-the-release)
  - [Publish release to SVN](#publish-release-to-svn)
  - [Publish release tag](#publish-release-tag)
  - [Publish documentation](#publish-documentation)
  - [Notify developers of release](#notify-developers-of-release)
  - [Update Announcements page](#update-announcements-page)
  - [Create release on GitHub](#create-release-on-github)
  - [Close the milestone](#close-the-milestone)
  - [Announce the release on the community slack](#announce-the-release-on-the-community-slack)
  - [Tweet about the release](#tweet-about-the-release)
  - [Bump chart version in Chart.yaml](#bump-chart-version-in-chartyaml)
  - [Remove old releases](#remove-old-releases)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

You can find the prerequisites to release Apache Airflow in [README.md](README.md). This document
details the steps for releasing Helm Chart.

# Prepare the Apache Airflow Helm Chart Release Candidate

## Pre-requisites

- Helm version >= 3.5.4

## Build Changelog

Before creating the RC, you need to build and commit the changelog for the release. For example, to list the
commits between the last release, `1.1.0`, and `main`:

```shell
git log --oneline helm-chart/1.1.0..main --pretty='format:- %s' -- chart/ docs/helm-chart/
```

## Build RC artifacts

The Release Candidate artifacts we vote upon should be the exact ones we vote against,
without any modification than renaming – i.e. the contents of the files must be
the same between voted release candidate and final release.
Because of this the version in the built artifacts that will become the
official Apache releases must not include the rcN suffix.

- Set environment variables

    ```shell
    # Set Version
    export VERSION=1.0.1rc1
    export VERSION_WITHOUT_RC=${VERSION%rc?}

    # Set AIRFLOW_REPO_ROOT to the path of your git repo
    export AIRFLOW_REPO_ROOT=$(pwd)

    # Example after cloning
    git clone https://github.com/apache/airflow.git airflow
    cd airflow
    export AIRFLOW_REPO_ROOT=$(pwd)
    ```

- We currently release Helm Chart from `main` branch:

    ```shell
    git checkout main
    ```

- Clean the checkout: the sdist step below will

    ```shell
    git clean -fxd
    ```

- Update Helm Chart version in `Chart.yaml`, example: `version: 1.0.0` (without
  the RC tag). If the default version of Airflow is different from `appVersion` change it.

- Commit the version change.

- Tag your release

    ```shell
    git tag -s helm-chart/${VERSION}
    ```

- Tarball the repo

    ```shell
    git archive --format=tar.gz helm-chart/${VERSION} --prefix=airflow-chart-${VERSION_WITHOUT_RC}/ \
        -o airflow-chart-${VERSION_WITHOUT_RC}-source.tar.gz chart .rat-excludes
    ```

- Generate chart binary

    NOTE: Make sure your checkout is clean at this stage - any untracked or changed files will otherwise be included
     in the file produced.

    Replace key email to your email address and path of keyring if it is different.
    (If you have not generated a key yet, generate it by following instructions on
    http://www.apache.org/dev/openpgp.html#key-gen-generate-key)

    ```shell
    helm package chart --dependency-update --sign --key "kaxilnaik@apache.org" \
        --keyring ~/.gnupg/secring.gpg
    ```

    Warning: the GnuPG v2 store your secret keyring using a new format kbx on the default
    location `~/.gnupg/pubring.kbx`. Please use the following command to convert your keyring to the
    legacy gpg format and run the above command again:

    ```shell
    gpg --export-secret-keys > ~/.gnupg/secring.gpg
    ```

    This should also generate Provenance file (Example: `airflow-1.0.0.tgz.prov`) as described in
    https://helm.sh/docs/topics/provenance/ which can be used to verify integrity of the Helm chart.

    Verify the signed chart:

    ```shell
    helm verify airflow-${VERSION_WITHOUT_RC}.tgz --keyring ~/.gnupg/secring.gpg
    ```

    Example Output:

    ```shell
    $ helm verify airflow-${VERSION_WITHOUT_RC}.tgz --keyring ~/.gnupg/secring.gpg
    Signed by: Kaxil Naik <kaxilnaik@apache.org>
    Signed by: Kaxil Naik <kaxilnaik@gmail.com>
    Using Key With Fingerprint: CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
    Chart Hash Verified: sha256:6185e54735e136d7d30d329cd16555a3a6c951be876aca8deac2022ab0568e53
    ```


- Generate SHA512/ASC

    ```shell
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh airflow-chart-${VERSION_WITHOUT_RC}-source.tar.gz
    ${AIRFLOW_REPO_ROOT}/dev/sign.sh airflow-${VERSION_WITHOUT_RC}.tgz
    ```

- Move the artifacts to ASF dev dist repo, Generate convenience `index.yaml` & Publish them

  ```shell
  # First clone the repo
  svn checkout https://dist.apache.org/repos/dist/dev/airflow airflow-dev

  # Create new folder for the release
  cd airflow-dev/helm-chart
  svn mkdir ${VERSION}

  # Move the artifacts to svn folder
  mv ${AIRFLOW_REPO_ROOT}/airflow-${VERSION_WITHOUT_RC}.tgz* ${VERSION}/
  mv ${AIRFLOW_REPO_ROOT}/airflow-chart-${VERSION_WITHOUT_RC}-source.tar.gz* ${VERSION}/
  cd ${VERSION}

  ###### Generate index.yaml file - Start
  # Download the latest index.yaml on Airflow Website
  curl https://airflow.apache.org/index.yaml --output index.yaml

  # Replace the URLs from "https://downloads.apache.org" to "https://archive.apache.org"
  # as the downloads.apache.org only contains latest releases.
  sed -i 's|https://downloads.apache.org/airflow/helm-chart/|https://archive.apache.org/dist/airflow/helm-chart/|' index.yaml

  # Generate / Merge the new version with existing index.yaml
  helm repo index --merge ./index.yaml . --url "https://dist.apache.org/repos/dist/dev/airflow/helm-chart/${VERSION}"

  ###### Generate index.yaml file - End

  # Commit the artifacts
  svn add *
  svn commit -m "Add artifacts for Helm Chart ${VERSION}"
  ```

- Remove old Helm Chart versions from the dev repo

  ```shell
  cd ..
  export PREVIOUS_VERSION=1.0.0rc1
  svn rm ${PREVIOUS_VERSION}
  svn commit -m "Remove old Helm Chart release: ${PREVIOUS_VERSION}"
  ```

- Push Tag for the release candidate

  ```shell
  cd ${AIRFLOW_REPO_ROOT}
  git push origin helm-chart/${VERSION}
  ```

## Prepare Vote email on the Apache Airflow release candidate

- Send out a vote to the dev@airflow.apache.org mailing list:

Subject:

```shell
cat <<EOF
[VOTE] Release Apache Airflow Helm Chart ${VERSION_WITHOUT_RC} based on ${VERSION}
EOF
```

Body:

```shell
cat <<EOF
Hello Apache Airflow Community,

This is a call for the vote to release Helm Chart version ${VERSION_WITHOUT_RC}.

The release candidate is available at:
https://dist.apache.org/repos/dist/dev/airflow/helm-chart/$VERSION/

airflow-chart-${VERSION_WITHOUT_RC}-source.tar.gz - is the "main source release" that comes with INSTALL instructions.
airflow-${VERSION_WITHOUT_RC}.tgz - is the binary Helm Chart release.

Public keys are available at: https://www.apache.org/dist/airflow/KEYS

For convenience "index.yaml" has been uploaded (though excluded from voting), so you can also run the below commands.

helm repo add apache-airflow-dev https://dist.apache.org/repos/dist/dev/airflow/helm-chart/$VERSION/
helm repo update
helm install airflow apache-airflow-dev/airflow

airflow-${VERSION_WITHOUT_RC}.tgz.prov - is also uploaded for verifying Chart Integrity, though not strictly required for releasing the artifact based on ASF Guidelines.

$ helm verify airflow-${VERSION_WITHOUT_RC}.tgz --keyring  ~/.gnupg/secring.gpg
Signed by: Kaxil Naik <kaxilnaik@apache.org>
Signed by: Kaxil Naik <kaxilnaik@gmail.com>
Using Key With Fingerprint: CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
Chart Hash Verified: sha256:6cd3f13fc93d60424a771a1a8a4121c4439f7b6b48fab946436da0ab70d5a507

The vote will be open for at least 72 hours (2021-05-19 01:30 UTC) or until the necessary number of votes are reached.

https://www.timeanddate.com/countdown/to?iso=20210519T0230&p0=136&font=cursive

Please vote accordingly:

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason

Only votes from PMC members are binding, but members of the community are
encouraged to test the release and vote with "(non-binding)".

For license checks, the .rat-excludes files is included, so you can run the following to verify licenses (just update $PATH_TO_RAT):

tar -xvf airflow-chart-${VERSION_WITHOUT_RC}-source.tar.gz
cd airflow-chart-${VERSION_WITHOUT_RC}
java -jar $PATH_TO_RAT/apache-rat-0.13/apache-rat-0.13.jar chart -E .rat-excludes

Please note that the version number excludes the \`rcX\` string, so it's now
simply ${VERSION_WITHOUT_RC}. This will allow us to rename the artifact without modifying
the artifact checksums when we actually release.

Thanks,
<your name>
EOF
```

Note, you need to update the `helm verify` output and the end of the voting period in the body.

# Verify the release candidate by PMCs

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

The following files should be present (7 files):

* `airflow-chart-${VERSION_WITHOUT_RC}-source.tar.gz` + .asc + .sha512
* `airflow-{VERSION_WITHOUT_RC}.tgz` + .asc + .sha512
* `airflow-{VERSION_WITHOUT_RC}.tgz.prov`

As a PMC you should be able to clone the SVN repository:

```shell
svn co https://dist.apache.org/repos/dist/dev/airflow
```

Or update it if you already checked it out:

```shell
svn update .
```

## Licence check

This can be done with the Apache RAT tool.

* Download the latest jar from https://creadur.apache.org/rat/download_rat.cgi (unpack the binary,
  the jar is inside)
* Unpack the release source archive (the `<package + version>-source.tar.gz` file) to a folder
* Enter the sources folder run the check

```shell
java -jar $PATH_TO_RAT/apache-rat-0.13/apache-rat-0.13.jar chart -E .rat-excludes
```

where `.rat-excludes` is the file in the root of Chart source code.

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
Checking airflow-1.0.0.tgz.asc
gpg: assuming signed data in 'airflow-1.0.0.tgz'
gpg: Signature made Sun 16 May 01:25:24 2021 BST
gpg:                using RSA key CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
gpg:                issuer "kaxilnaik@apache.org"
gpg: Good signature from "Kaxil Naik <kaxilnaik@apache.org>" [unknown]
gpg:                 aka "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: The key's User ID is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: CDE1 5C6E 4D3A 8EC4 ECF4  BA4B 6674 E08A D7DE 406F

Checking airflow-chart-1.0.0-source.tar.gz.asc
gpg: assuming signed data in 'airflow-chart-1.0.0-source.tar.gz'
gpg: Signature made Sun 16 May 02:24:09 2021 BST
gpg:                using RSA key CDE15C6E4D3A8EC4ECF4BA4B6674E08AD7DE406F
gpg:                issuer "kaxilnaik@apache.org"
gpg: Good signature from "Kaxil Naik <kaxilnaik@apache.org>" [unknown]
gpg:                 aka "Kaxil Naik <kaxilnaik@gmail.com>" [unknown]
gpg: WARNING: The key's User ID is not certified with a trusted signature!
gpg:          There is no indication that the signature belongs to the owner.
Primary key fingerprint: CDE1 5C6E 4D3A 8EC4 ECF4  BA4B 6674 E08A D7DE 406F
```

## SHA512 sum check

Run this:

```shell
for i in *.sha512
do
    echo "Checking $i"; shasum -a 512 `basename $i .sha512 ` | diff - $i
done
```

You should get output similar to:

```
Checking airflow-1.0.0.tgz.sha512
Checking airflow-chart-1.0.0-source.tar.gz.sha512
```

# Verify release candidates by Contributors

Contributors can run below commands to test the Helm Chart

```shell
helm repo add apache-airflow-dev https://dist.apache.org/repos/dist/dev/airflow/helm-chart/1.0.1rc1/
helm repo update
helm install airflow apache-airflow-dev/airflow
```

You can then perform any other verifications to check that it works as you expected by
upgrading the Chart or installing by overriding default of `values.yaml`.

# Publish the final release

## Summarize the voting for the release

Once the vote has been passed, you will need to send a result vote to dev@airflow.apache.org:

Subject:

```
[RESULT][VOTE] Release Apache Airflow Helm Chart 1.0.1 based on 1.0.1rc1
```

Message:

```
Hello all,

The vote to release Apache Airflow Helm Chart version 1.0.1 based on 1.0.1rc1 is now closed.

The vote PASSED with 4 binding "+1", 4 non-binding "+1" and 0 "-1" votes:

"+1" Binding votes:

  - Kaxil Naik
  - Jarek Potiuk
  - Ash Berlin-Taylor
  - Xiaodong Deng

"+1" Non-Binding votes:

  - Jed Cunningham
  - Ephraim Anierobi
  - Dennis Akpenyi
  - Ian Stanton

Vote thread:
https://lists.apache.org/thread.html/r865f041e491a2a7a52e17784abf0d0f2e35c3bac5ae8a05927285558%40%3Cdev.airflow.apache.org%3E

I'll continue with the release process and the release announcement will follow shortly.

Thanks,
<your name>
```

## Publish release to SVN

You need to migrate the RC artifacts that passed to this repository:
https://dist.apache.org/repos/dist/release/airflow/helm-chart/
(The migration should include renaming the files so that they no longer have the RC number in their filenames.)

The best way of doing this is to svn cp between the two repos (this avoids having to upload
the binaries again, and gives a clearer history in the svn commit logs):

```shell
# First clone the repo
export RC=1.0.1rc1
export VERSION=${RC/rc?/}
svn checkout https://dist.apache.org/repos/dist/release/airflow airflow-release

# Create new folder for the release
cd airflow-release/helm-chart
export AIRFLOW_SVN_RELEASE_HELM=$(pwd)
svn mkdir ${VERSION}
cd ${VERSION}

# Move the artifacts to svn folder & commit (don't copy or copy & remove - index.yaml)
for f in ../../../airflow-dev/helm-chart/$RC/*; do svn cp $f ${$(basename $f)/}; done
svn rm index.yaml
svn commit -m "Release Airflow Helm Chart Check ${VERSION} from ${RC}"

```

Verify that the packages appear in [Airflow Helm Chart](https://dist.apache.org/repos/dist/release/airflow/helm-chart/).

## Publish release tag

Create and push the release tag:

```shell
cd "${AIRFLOW_REPO_ROOT}"
git checkout helm-chart/${RC}
git tag -s helm-chart/${VERSION} -m "Apache Airflow Helm Chart ${VERSION}"
git push origin helm-chart/${VERSION}
```

## Publish documentation

In our cases, documentation for the released versions is published in a separate repository -
[`apache/airflow-site`](https://github.com/apache/airflow-site), but the documentation source code and
build tools are available in the `apache/airflow` repository, so you have to coordinate
between the two repositories to be able to build the documentation.

- First, copy the airflow-site repository, create branch, and set the environment variable ``AIRFLOW_SITE_DIRECTORY``.

    ```shell
    git clone https://github.com/apache/airflow-site.git airflow-site
    cd airflow-site
    git checkout -b helm-${VERSION}-docs
    export AIRFLOW_SITE_DIRECTORY="$(pwd)"
    ```

- Then you can go to the directory and build the necessary documentation packages

    ```shell
    cd "${AIRFLOW_REPO_ROOT}"
    git checkout helm-chart/${VERSION}
    ./breeze build-docs -- --package-filter helm-chart --for-production
    ```

- Now you can preview the documentation.

    ```shell
    ./docs/start_doc_server.sh
    ```

- Copy the documentation to the ``airflow-site`` repository.

    ```shell
    ./docs/publish_docs.py --package-filter helm-chart
    ```

- Update `index.yaml`

  Regenerate `index.yaml` so it can be added to the Airflow website to allow: `helm repo add https://airflow.apache.org`.

    ```shell
    cd "${AIRFLOW_SITE_DIRECTORY}"
    curl https://dist.apache.org/repos/dist/dev/airflow/helm-chart/$RC/index.yaml -o index.yaml
    cp ${AIRFLOW_SVN_RELEASE_HELM}/${VERSION}/airflow-${VERSION}.tgz .
    helm repo index --merge ./index.yaml . --url "https://downloads.apache.org/airflow/helm-chart/$VERSION"
    rm airflow-${VERSION}.tgz
    mv index.yaml landing-pages/site/static/index.yaml
    ```

- Commit new docs, push, and open PR

    ```shell
    git add .
    git commit -m "Add documentation for Apache Airflow Helm Chart ${VERSION}"
    git push
    # and finally open a PR
    ```

## Notify developers of release

- Notify users@airflow.apache.org (cc'ing dev@airflow.apache.org) that
the artifacts have been published:

Subject:

```shell
cat <<EOF
[ANNOUNCE] Apache Airflow Helm Chart version ${VERSION} Released
EOF
```

Body:

```shell
cat <<EOF
Dear Airflow community,

I am pleased to announce that we have released Apache Airflow Helm chart $VERSION 🎉 🎊

The source release, as well as the "binary" Helm Chart release, are available:

📦   Official Sources: https://airflow.apache.org/docs/helm-chart/$VERSION/installing-helm-chart-from-sources.html
📦   ArtifactHub: https://artifacthub.io/packages/helm/apache-airflow/airflow
📚   Docs: https://airflow.apache.org/docs/helm-chart/$VERSION/
🚀   Quick Start Installation Guide: https://airflow.apache.org/docs/helm-chart/$VERSION/quick-start.html
🛠️   Changelog: https://airflow.apache.org/docs/helm-chart/$VERSION/changelog.html

Thanks to all the contributors who made this possible.

Cheers,
<your name>
EOF
```

Send the same email to announce@apache.org, except change the opening line to `Dear community,`.
It is more reliable to set it via the web ui at https://lists.apache.org/list.html?announce@apache.org

## Update Announcements page

Update "Announcements" page at the [Official Airflow website](https://airflow.apache.org/announcements/)

## Create release on GitHub

Create a new release on GitHub with the changelog and assets from the release svn.

## Close the milestone

Close the milestone on GitHub. Create the next one if it hasn't been already (it probably has been).

## Announce the release on the community slack

Post this in the #announce channel:

```shell
cat <<EOF
We’ve just released Apache Airflow Helm Chart ${VERSION} 🎉

📦 ArtifactHub: https://artifacthub.io/packages/helm/apache-airflow/airflow
📚 Docs: https://airflow.apache.org/docs/helm-chart/$VERSION/
🚀 Quick Start Installation Guide: https://airflow.apache.org/docs/helm-chart/$VERSION/quick-start.html
🛠 Changelog: https://airflow.apache.org/docs/helm-chart/$VERSION/changelog.html

Thanks to all the contributors who made this possible.
EOF
```

## Tweet about the release

Tweet about the release:

```shell
cat <<EOF
We've just released Apache Airflow Helm chart $VERSION 🎉

📦 ArtifactHub: https://artifacthub.io/packages/helm/apache-airflow/airflow
📚 Docs: https://airflow.apache.org/docs/helm-chart/$VERSION/
🛠️ Changelog: https://airflow.apache.org/docs/helm-chart/$VERSION/changelog.html

Thanks to all the contributors who made this possible.
EOF
```

## Bump chart version in Chart.yaml

Bump the chart version to the next version in `chart/Chart.yaml` in main.


## Remove old releases

We should keep the old version a little longer than a day or at least until the updated
``index.yaml`` is published. This is to avoid errors for users who haven't run ``helm repo update``.

It is probably ok if we leave last 2 versions on release svn repo too.

```shell
# http://www.apache.org/legal/release-policy.html#when-to-archive
cd airflow-release/helm-chart
export PREVIOUS_VERSION=1.0.0
svn rm ${PREVIOUS_VERSION}
svn commit -m "Remove old Helm Chart release: ${PREVIOUS_VERSION}"
```
