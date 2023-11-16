#!/usr/bin/env python
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

import os
import subprocess
from datetime import datetime

AIRFLOW_SOURCES = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))

# Check common named remotes for the upstream repo
remotes = ["origin", "apache"]
for remote in remotes:
    try:
        output = subprocess.check_output(["git", "remote", "get-url", "--push", remote], stderr=subprocess.DEVNULL).decode().strip()
        if "apache/airflow.git" in output:
            break
    except subprocess.CalledProcessError:
        pass
else:
    raise ValueError("Could not find remote configured to push to apache/airflow")

tags = []
for file in os.listdir(os.path.join(AIRFLOW_SOURCES, "dist")):
    if file.endswith(".whl") and "airflow_providers_" in file and "-py3" in file:
        matches = re.search(r"airflow_providers_(.*)-(.*)-py3", file)
        if matches:
            provider = f"providers-{matches.group(1).replace('_', '-')}"
            tag = f"{provider}/{matches.group(2)}"
            try:
                subprocess.check_call(["git", "tag", tag, "-m", f"Release {datetime.now().strftime('%Y-%m-%d')} of providers"])
                tags.append(tag)
            except subprocess.CalledProcessError:
                pass

if tags:
    try:
        subprocess.check_call(["git", "push", remote] + tags)
        print("Tags pushed successfully")
    except subprocess.CalledProcessError:
        print("Failed to push tags, probably a connectivity issue to Github")
        CLEAN_LOCAL_TAGS = os.environ.get("CLEAN_LOCAL_TAGS", "true")
        if CLEAN_LOCAL_TAGS == "true":
            print("Cleaning up local tags...")
            for tag in tags:
                subprocess.run(["git", "tag", "-d", tag])
        else:
            print("Local tags are not cleaned up, unset CLEAN_LOCAL_TAGS or set to true")
