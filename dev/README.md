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
  - [Airflow Pull Request Tool](#airflow-pull-request-tool)
  - [Airflow release signing tool](#airflow-release-signing-tool)
- [Verifying the release candidate by PMCs (legal)](#verifying-the-release-candidate-by-pmcs-legal)
  - [PMC voting](#pmc-voting)
  - [SVN check](#svn-check)
  - [Verifying the licences](#verifying-the-licences)
  - [Verifying the signatures](#verifying-the-signatures)
  - [Verifying the SHA512 sum](#verifying-the-sha512-sum)
- [Verifying if the release candidate "works" by Contributors](#verifying-if-the-release-candidate-works-by-contributors)

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
