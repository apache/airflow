#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import datetime as date
import os
import re
import subprocess

AIRFLOW_SOURCES = os.path.abspath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "..",
    )
)

remotes = ["origin", "apache"]
for remote in remotes:
    try:
        command = ["git", "remote", "get-url", "--push", remote]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        if "apache/airflow.git" in result.stdout:
            found_remote = remote
            break
    except subprocess.CalledProcessError:
        pass

if found_remote is None:
    raise ValueError("Could not find remote configured to push to apache/airflow")

tags = []
for file in os.listdir(os.path.join(AIRFLOW_SOURCES, "dist")):
    if file.endswith(".whl"):
        file_path = os.path.join(AIRFLOW_SOURCES, "dist", file)
        match = re.match(r".*airflow_providers_(.*)-(.*)-py3.*", file)
        if match:
            provider = f"providers-{match.group(1).replace('_', '-')}"
            tag = f"{provider}/{match.group(2)}"
            try:
                subprocess.run(
                    ["git", "tag", tag, "-m", f"Release {date.datetime.today()} of providers"], check=True
                )
                tags.append(tag)
            except subprocess.CalledProcessError:
                pass

if tags and len(tags) > 0:
    try:
        push_command = ["git", "push", remote] + tags
        push_result = subprocess.Popen(
            push_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        push_output, push_error = push_result.communicate()
        if push_output:
            print(push_output)
        if push_error:
            print(push_error)
        print("Tags pushed successfully")
    except subprocess.CalledProcessError:
        print("Failed to push tags, probably a connectivity issue to Github")
        clean_local_tags = os.environ.get("CLEAN_LOCAL_TAGS", "true").lower() == "true"
        if clean_local_tags:
            for tag in tags:
                try:
                    subprocess.run(["git", "tag", "-d", tag], check=True)
                except subprocess.CalledProcessError:
                    pass
            print("Cleaning up local tags...")
        else:
            print("Local tags are not cleaned up, unset CLEAN_LOCAL_TAGS or set to true")
