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
  - [Build Release Notes](#build-release-notes)
  - [Build RC artifacts](#build-rc-artifacts)
  - [Prepare issue for testing status of rc](#prepare-issue-for-testing-status-of-rc)
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
  - [Add release data to Apache Committee Report Helper](#add-release-data-to-apache-committee-report-helper)
  - [Update Announcements page](#update-announcements-page)
  - [Create release on GitHub](#create-release-on-github)
  - [Close the milestone](#close-the-milestone)
  - [Close the testing status issue](#close-the-testing-status-issue)
  - [Update issue template with the new release](#update-issue-template-with-the-new-release)
  - [Announce the release on the community slack](#announce-the-release-on-the-community-slack)
  - [Announce about the release in social media](#announce-about-the-release-in-social-media)
  - [Bump chart version in Chart.yaml](#bump-chart-version-in-chartyaml)
  - [Remove old releases](#remove-old-releases)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

You can find the prerequisites to release Apache Airflow in [README.md](README.md). This document
details the steps for releasing Helm Chart.

# Prepare the Apache Airflow Helm Chart Release Candidate

## Pre-requisites

- Helm version >= 3.5.4

## Build Release Notes

Before creating the RC, you need to build and commit the release notes for the release:

Preview with:

```shell script
towncrier build --draft --version=${VERSION_WITHOUT_RC} --date=2021-12-15 --dir chart --config chart/newsfragments/config.toml
```

Then remove the `--draft` flag to have towncrier build the release notes for real.

If no significant changes where added in this release, add the header and put "No significant changes.".

### Add changelog annotations to `Chart.yaml`

Once the release notes have been built, run the script to generate the changelog annotations.

```shell
./dev/chart/build_changelog_annotations.py
```

Verify the output looks right (only entries from this release), then put them in `Chart.yaml`, for example:

```yaml
annotations:
  artifacthub.io/changes: |
    - kind: added
      description: Add resources for `cleanup` and `createuser` jobs
      links:
        - name: "#19263"
          url: https://github.com/apache/airflow/pull/19263
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

- Add and commit the version change.

    ```shell
    git add chart
    git commit -m "Chart: Bump version to $VERSION_WITHOUT_RC"
    ```

  Note: You will tag this commit, you do not need to open a PR for it.

- Tag your release

    ```shell
    git tag -s helm-chart/${VERSION} -m "Apache Airflow Helm Chart $VERSION"
    ```

- Tarball the repo

    NOTE: Make sure your checkout is clean at this stage - any untracked or changed files will otherwise be included
     in the file produced.

    ```shell
    git archive --format=tar.gz helm-chart/${VERSION} --prefix=airflow-chart-${VERSION_WITHOUT_RC}/ \
        -o airflow-chart-${VERSION_WITHOUT_RC}-source.tar.gz chart .rat-excludes
    ```

- Generate chart binary


    ```shell
    helm package chart --dependency-update
    ```

- Sign the chart binary

    In the following command, replace the email address with your email address or your KEY ID
    so GPG uses the right key to sign the chart.
    (If you have not generated a key yet, generate it by following instructions on
    http://www.apache.org/dev/openpgp.html#key-gen-generate-key)

    ```shell
    helm gpg sign -u jedcunningham@apache.org airflow-${VERSION_WITHOUT_RC}.tgz
    ```

    Warning: you need the `helm gpg` plugin to sign the chart. It can be found at: https://github.com/technosophos/helm-gpg

    This should also generate a provenance file (Example: `airflow-1.0.0.tgz.prov`) as described in
    https://helm.sh/docs/topics/provenance/, which can be used to verify integrity of the Helm chart.

    Verify the signed chart (with example output shown):

    ```shell
    $ helm gpg verify airflow-${VERSION_WITHOUT_RC}.tgz
    gpg: Signature made Thu Jan  6 21:33:35 2022 MST
    gpg:                using RSA key E1A1E984F55B8F280BD9CBA20BB7163892A2E48E
    gpg: Good signature from "Jed Cunningham <jedcunningham@apache.org>" [ultimate]
    plugin: Chart SHA verified. sha256:b33eac716e0416a18af89fb4fa1043fcfcf24f9f903cda3912729815213525df
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
  git push origin tag helm-chart/${VERSION}
  ```

## Prepare issue for testing status of rc

Create an issue for testing status of the RC (PREVIOUS_RELEASE should be the previous release version
(for example 1.4.0).

```shell script
cat <<EOF
Status of testing of Apache Airflow Helm Chart ${VERSION}
EOF
```

Content is generated with:

```shell
./dev/prepare_release_issue.py generate-issue-content --previous-release helm-chart/<PREVIOUS_RELEASE> \
    --current-release helm-chart/${VERSION} --is-helm-chart

```

Copy the URL of the issue.

## Prepare Vote email on the Apache Airflow release candidate

- Send out a vote to the dev@airflow.apache.org mailing list:

Subject:

```shell
cat <<EOF
[VOTE] Release Apache Airflow Helm Chart ${VERSION_WITHOUT_RC} based on ${VERSION}
EOF
```

```shell
export VOTE_END_TIME=$(date --utc -d "now + 72 hours + 10 minutes" +'%Y-%m-%d %H:%M')
export TIME_DATE_URL="to?iso=$(date --utc -d "now + 72 hours + 10 minutes" +'%Y%m%dT%H%M')&p0=136&font=cursive"
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

$ helm gpg verify airflow-${VERSION_WITHOUT_RC}.tgz
gpg: Signature made Thu Jan  6 21:33:35 2022 MST
gpg:                using RSA key E1A1E984F55B8F280BD9CBA20BB7163892A2E48E
gpg: Good signature from "Jed Cunningham <jedcunningham@apache.org>" [ultimate]
plugin: Chart SHA verified. sha256:b33eac716e0416a18af89fb4fa1043fcfcf24f9f903cda3912729815213525df

The vote will be open for at least 72 hours ($VOTE_END_TIME UTC) or until the necessary number of votes is reached.

https://www.timeanddate.com/countdown/$TIME_DATE_URL

Please vote accordingly:

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason

Only votes from PMC members are binding, but members of the community are
encouraged to test the release and vote with "(non-binding)".

Consider this my (binding) +1.

For license checks, the .rat-excludes files is included, so you can run the following to verify licenses (just update your path to rat):

tar -xvf airflow-chart-${VERSION_WITHOUT_RC}-source.tar.gz
cd airflow-chart-${VERSION_WITHOUT_RC}
java -jar apache-rat-0.13.jar chart -E .rat-excludes

Please note that the version number excludes the \`rcX\` string, so it's now
simply ${VERSION_WITHOUT_RC}. This will allow us to rename the artifact without modifying
the artifact checksums when we actually release it.

The status of testing the Helm Chart by the community is kept here:
<TODO COPY LINK TO THE ISSUE CREATED>

Thanks,
<your name>
EOF
```

Note, you need to update the `helm gpg verify` output and verify the end of the voting period in the body.

Note, For RC2/3 you may refer to shorten vote period as agreed in mailing list [thread](https://lists.apache.org/thread/cv194w1fqqykrhswhmm54zy9gnnv6kgm).

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
    breeze build-docs --package-filter helm-chart --clean-build --for-production
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
🛠️   Release Notes: https://airflow.apache.org/docs/helm-chart/$VERSION/release_notes.html

Thanks to all the contributors who made this possible.

Cheers,
<your name>
EOF
```

Send the same email to announce@apache.org, except change the opening line to `Dear community,`.
It is more reliable to send it via the web ui at https://lists.apache.org/list.html?announce@apache.org
(press "c" to compose a new thread)

## Add release data to Apache Committee Report Helper

Add the release data (version and date) at: https://reporter.apache.org/addrelease.html?airflow

## Update Announcements page

Update "Announcements" page at the [Official Airflow website](https://airflow.apache.org/announcements/)

## Create release on GitHub

Create a new release on GitHub with the release notes and assets from the release svn.

## Close the milestone

Close the milestone on GitHub. Create the next one if it hasn't been already (it probably has been).
Update the new milestone in the [*Currently we are working on* issue](https://github.com/apache/airflow/issues/10176)
make sure to update the last updated timestamp as well.

## Close the testing status issue

Don't forget to thank the folks who tested and close the issue tracking the testing status.

## Update issue template with the new release

Updating issue templates in `.github/ISSUE_TEMPLATE/airflow_helmchart_bug_report.yml` with the new version

## Announce the release on the community slack

Post this in the #announce channel:

```shell
cat <<EOF
We've just released Apache Airflow Helm Chart ${VERSION} 🎉

📦 ArtifactHub: https://artifacthub.io/packages/helm/apache-airflow/airflow
📚 Docs: https://airflow.apache.org/docs/helm-chart/$VERSION/
🚀 Quick Start Installation Guide: https://airflow.apache.org/docs/helm-chart/$VERSION/quick-start.html
🛠 Release Notes: https://airflow.apache.org/docs/helm-chart/$VERSION/release_notes.html

Thanks to all the contributors who made this possible.
EOF
```

## Announce about the release in social media

------------------------------------------------------------------------------------------------------------
Announcement is done from official Apache-Airflow accounts.

* Twitter: https://twitter.com/ApacheAirflow
* Linkedin: https://www.linkedin.com/company/apache-airflow/

If you don't have access to the account ask PMC to post.

------------------------------------------------------------------------------------------------------------

Tweet and post on Linkedin about the release:

```shell
cat <<EOF
We've just released Apache Airflow Helm chart $VERSION 🎉

📦 ArtifactHub: https://artifacthub.io/packages/helm/apache-airflow/airflow
📚 Docs: https://airflow.apache.org/docs/helm-chart/$VERSION/
🛠️ Release Notes: https://airflow.apache.org/docs/helm-chart/$VERSION/release_notes.html

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
