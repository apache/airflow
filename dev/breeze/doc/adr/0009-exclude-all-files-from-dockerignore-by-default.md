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

- [9. Exclude all files from dockerignore by default](#9-exclude-all-files-from-dockerignore-by-default)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 9. Exclude all files from dockerignore by default

Date: 2022-02-13

## Status

Accepted

## Context

Building Docker container always starts with sending the build
context first - depending on a number of files in the context.
The context might have even many hundreds of MB, which -
depending on where your docker builder is and how fast your
system is - might lead to even tens of seconds of delays before
the docker build command is run and the actual build starts.

The context has to be compressed, sent, decompressed, so it
takes CPU, networking and I/O.

Airflow - unfortunately has Dockerfiles, and sources
directly in the top-level of the project. There is no "src"
folder and by default the docker commands use the folder
where the "Dockerfile" is placed and the context files cannot
be taken from outside the context. Thus - Dockerfiles have
to be put at the "top-level" of the airflow project.

By default, all files in the current context should be sent
as context unless you ignore them via .dockerfile - in a
concept that is similar to .gitignore. Airflow has many
files that are huge and generated during running and building
(for example node_modules) but also `.egginfo` and plenty of
other files in many folders.

It sounds like a reasonable approach to do to ignore specific
folders, however it has one  drawback. You might simply not
realise that some newly generated files have been added and
increase the context - thus increase the overhead needed to
build the docker images. There is no way to prevent or check
such accidental additions - for example when refactoring
files, or adding new functionalities.

## Decision

There are a number of strategies that can address the problem,
ranging by convention and automated checks but in our case,
we have multiple independent contributors and committers
reviewing the code, such a change might easily slip-through.
So the solution should be "self-managing". Luckily, there is
a way that has been discussed in a number of places but
[notably here](https://stackoverflow.com/questions/28097064/dockerignore-ignore-everything-except-a-file-and-the-dockerfile)

The strategy involves ignoring all files by default and only
selectively excluding certain folders and patterns that should
be allowed to be part of the context.

The `.dockerignore` file has
[appropriate functionality](https://docs.docker.com/engine/reference/builder/#dockerignore-file)

This is what we decided to use for our Dockerfile and Dockerfile.ci.

## Consequences

There are two consequences of this decision:

* whenever new files (that do not follow the "approved" patterns
  will be added to the airflow repository, they will not increase
  the size of the context
* we have to still regularly monitor the context to see whether
  the approved patterns did not - by accident approved some
  unnecessary files, but that should be a rather rare event
* whenever someone wants to add something to our container images,
  and it is not a part of already "approved" patterns, the file
  will be missing during the build, which might lead to a little
  surprise, but it is explained in the `.dockerignore` what to do
  in this case, and `.dockerignore` is the place where you would
  look for a problem anyway. The users should be guided to add
  new pattern to the `.dockerignore` in this case.
