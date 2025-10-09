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

- [8. Fixing group permissions before build](#8-fixing-group-permissions-before-build)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 8. Fixing group permissions before build

Date: 2022-02-13

## Status

Accepted

## Context

The problem to solve is the fact that depending on configuration of the local
distribution of Linux, remote cache can be invalidated resulting in
rebuilding most of the image, even if the files that are used for the
build have not changed.


It is a result of a few combined issues.

1) Git does not store "group write" permissions for files you put in the
   repository. This is a historical decision also coming from the fact that
   "group" permissions only make sense for POSIX compliant systems. This
   Is not available for Windows, for example.

3) When the files are checked out, by default git uses "umask" of the current
   system to determine whether the group permissions to write are
   [set or not](https://git-scm.com/docs/git-config#Documentation/git-config.txt-coresharedRepository).
   That's the default behaviour when 'false' is set.

4) Unfortunately POSIX does not define what should be the default umask, It
   is usually either `0022` or `0002` - depending on the system. This basically
   mean that your file when you check it out with git will be "group read" when
   umask is `0022` but will be "group read-write" when umask is `0002`. More about
   umask and difference between those two settings can be found here:
   https://www.cyberciti.biz/tips/understanding-linux-unix-umask-value-usage.html


As the result, when you check out airflow project on `0022` system you will get this:


```
ls -la scripts/ci
total 132
drwxr-xr-x 19 jarek jarek  4096 Feb  5 20:49 .
drwxr-xr-x  8 jarek jarek  4096 Feb  5 20:49 ..
drwxr-xr-x  2 jarek jarek  4096 Feb  5 20:49 build_airflow
```

But when you check out airflow project on `0002` umask system, you will get
this: (note "w" in the group permission part):

```
ls -la scripts/ci
total 132
drwxrwxr-x 19 jarek jarek  4096 Feb  5 20:49 .
drwxrwxr-x  8 jarek jarek  4096 Feb  5 20:49 ..
drwxrwxr-x  2 jarek jarek  4096 Feb  5 20:49 build_airflow
```

Pretty much all Linux distributions use `0022` umask for root. But when it
comes to "regular" users, it is different - Ubuntu  up until recently used
"0002" for regular users, and Debian/Mint "0022". The result is - we cannot
rely on the "w" bit being set for files when users check it out - it might or
might not be set, and it's beyond of our control when `git checkout` is
executed.

On its own - this is not a problem, but it is a HUGE problem when it comes to
caching Docker builds. The problem is, that when you build Docker image, docker
actually uses the permissions on the file to determine whether a file changed
or not. In the case above. those two "build_airflow" files will have identical
content but Docker finds them "different"  (because of the "w" permission).

In this case whenever in Dockerfile we will use:

```
COPY scripts/ci/build_airflow /opt/airflow/scripts/ci/build
```

Docker "thinks" that the file has changed and will simply invalidate the cache.
So when we use remote cache (as we do) this means that docker will always
rebuild the docker image from that step, because it will not realise that this
is in fact the same file.  This is a huge problem in our case, because in order
to speed up local rebuilds of Breeze image. we use remote cache, and we have to
make sure that the file that was used to build airflow will be properly seen as
"unchanged" by anyone who builds the image locally - in order to heavily
optimize the build time. Pulling cached layers is usually much faster than
rebuilding them - especially that rebuild often involves pulling `pip` packages
and `apt` packages.

There is no other way to fix it but to make sure that write permissions for
all the files that can be potentially used during the Docker image build should
have the `write` group permission removed.

## Decision

Breeze should fix the problem just before building. We should find all files
and directories which are present in git repository and that have "w" bit set
and remove the bit. After this operation none of the files that are going to be
used for docker build will have the "w" bit set for them - and the cache is
invalidated.

Note! We cannot "blank" remove all the files in airflow directory, because
it can take a very long time when the user develops/builds Airflow. When you
develop  and run airflow locally, there are a lot of files created and
generated -  log files, build artifacts, but also node modules (a lot of) and
generated static files. Those files are not needed/used in the Docker build
(we make sure of that by removing all files by default and only selectively
adding files that we need). On a number of systems (especially those where
the filesystem is mounted to docker container via user filesystem - MacOS
or Windows) just changing permission of all those files can change tens of
seconds - so instead we opted to change only permissions of those files
that are added to Git.

## Consequences

As a consequence of this change, no matter what umask your system is configured
with, when you use remote cache and the "scripts" to build image have not been
modified, you will be able to use the remote cache and pull the layers that
were stored in the cache rather than rebuild it, which might save multiple minutes
on rebuilding the image (depending on the ratio of your network vs. disk/cpu
speed).
