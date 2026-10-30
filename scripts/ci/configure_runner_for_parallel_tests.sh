#!/usr/bin/env bash
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

# Large runners run a test type per CPU, each a docker compose project with its own database container.
#
# Every test type of a parallel test run is a docker compose project with a network of its own. Docker's
# default address pools hand out /16 and /20 subnets, which is about 30 networks per host, so large runners
# running a test type per CPU fail with "all predefined address pools have been fully subnetted".
# Splitting the same 172.17.0.0/12 range into /24 subnets gives 4096 networks without claiming new addresses.
set -euo pipefail

DAEMON_JSON=/etc/docker/daemon.json

sudo mkdir -p "$(dirname "${DAEMON_JSON}")"
sudo python3 - "${DAEMON_JSON}" <<'EOF'
import json
import pathlib
import sys

daemon_json = pathlib.Path(sys.argv[1])
config = json.loads(daemon_json.read_text()) if daemon_json.exists() and daemon_json.read_text().strip() else {}
config["default-address-pools"] = [{"base": "172.17.0.0/12", "size": 24}]
daemon_json.write_text(json.dumps(config, indent=2) + "\n")
print(daemon_json.read_text())
EOF
sudo systemctl restart docker

# Each MySQL server reserves Linux AIO contexts for InnoDB, and the default limit of 65536 runs out with a few
# dozen of them: "io_setup() failed with EAGAIN".
sudo sysctl -w fs.aio-max-nr=1048576
docker info --format '{{.ServerVersion}}' > /dev/null
