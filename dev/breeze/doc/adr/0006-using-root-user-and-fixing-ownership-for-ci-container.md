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

- [6. Using root user and fixing ownership for CI container](#6-using-root-user-and-fixing-ownership-for-ci-container)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Alternatives](#alternatives)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 6. Using root user and fixing ownership for CI container

Date: 2022-01-29

## Status

Accepted

## Context

Using root user to run containers is not recommended, however there are a
number of problems connected with ownership of files when host files and
directories are mounted to inside the container on Linux. Mounting files and
directories for development purposes is often used for development environments
to share sources with the container and to share results of tests/logs etc.
from the container to the host.

This is only a problem on Linux. The reason is that on Linux files that are
mounted from the host to the container are mounted using native filesystem.
This basically means that any file created inside the container will keep the
userid /group id that are used in container also in the host.

On MacOS and Windows this is not needed. Both MacOS and Windows use
"user-space" filesystems to mount files. The filesystems are far slower than
the native filesystem (many times actually) - which impacts the speed of
running Airflow in Docker container on MacOS and Windows. However, they
automatically remap the user - all the files created inside the containers are
automatically remapped to have the "host" user ownership and there is no
need to fix the ownership for those cases.

On Linux any file we create in container will keep the same user id and group
id in the host. But those user/group ids might not exist in the Host. - if we
create a user 50001 in the container, the id will remain like that on the host,
when we exit from the container. This is very problematic because when we map
"logs" directory and some logs (and directories) are created there, they might
be owned by a non-existing user after we exit. And we want to be able to see
the logs outside the container because that's where we usually have IDE and
that's where we keep reading those and analyse them.

Then, the problem is that if you want to delete such folders and files, you
need to use sudo in the host, because your regular user has no access to it.
This is big problem especially if files are created inside your source
directory (which is also mounted to the container). For example, it will
prevent you from switching branches easily because git will not be able to
remove some files, and it will refuse to switch branches. Unfortunately,
some tests and tools we use, generate files in the sources when
running tests (even if we try to remove that, some of that is impossible or
very difficult). Therefore, after running tests, by default, you might not
be able to switch between branches without manually removing some files with
sudo.

There is also "reverse" problem - if you create files in a host with no "all"
permissions, and you mount them inside the container, and container runs as
"different" user, the user in container cannot access to those file (unless you
run as root inside the container - root inside the container is equivalent to
root in host and can access and update all files).

## Decision


In order to avoid that we have a few things:

a) We use root user in container - all the files are created and run as root
   user. This is not recommended for production, but it is great for CI - because
   you can freely create and read any mounted files (no matter what user), you
   can also run pip/apt etc. without sudo. It is generally much more
   convenient for many development tasks. The side effect of that is that all
   files created in the container have root user/group set.

b) we pass `HOST_USER_ID` and `HOST_GROUP_ID` to the container, so that we know
   who is the user on the host. Depending on the linux distro and even
   depending on your configuration (how many users you have created and in
   which sequence) - the UID can be different.

c) when the user enters the container, we set a `trap`:

   `add_trap in_container_fix_ownership EXIT HUP INT TERM`

   This trap runs `fix_ownership` script that looks for all created files in
   the directories where we expect we
   will create files:

```
"/files"
"/root/.aws"
"/root/.azure"
"/root/.config/gcloud"
"/root/.docker"
"/opt/airflow/logs"
"/opt/airflow/docs"
"/opt/airflow/dags"
"${AIRFLOW_SOURCES}"
```

Whenever we exit, or terminate the container, this script is executed, and
it finds all files owned by "root" in those directories and changes their
ownership to be `HOST_USER/HOST_GROUP`. This way when you exit the
containers on linux, the files are owned by the host user, and can be
easily deleted - either manually or when you change branches.


## Alternatives

The problem could potentially be mitigated by
[user remapping]( https://docs.docker.com/engine/security/userns-remap/)
The problem is that it  can only be configured at the "docker daemon" level, and
this is something we should not require an average user should do, also the
problem with user remapping is that it is "global" setting. It will remap your
user for all containers and in many cases this is not what you really want.

## Consequences

* The Linux users do not have to worry about removing generated files using sudo
* The Linux users can switch between branches easily
* The Linux users can use logs and other results created in container without sudo
