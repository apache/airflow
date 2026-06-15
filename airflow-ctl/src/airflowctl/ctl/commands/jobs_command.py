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

import socket

import rich

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, provide_api_client


@provide_api_client(kind=ClientKind.CLI)
def check(args, api_client=NEW_API_CLIENT) -> None:
    """Check if job(s) are still alive."""
    if args.allow_multiple and args.limit <= 1:
        raise SystemExit("To use option --allow-multiple, you must set the limit to a value greater than 1.")
    if args.hostname and args.local:
        raise SystemExit("You can't use --hostname and --local at the same time")

    hostname = socket.getfqdn() if args.local else args.hostname
    alive_jobs = api_client.jobs.list(job_type=args.job_type, hostname=hostname, is_alive=True).jobs
    if args.limit > 0:
        alive_jobs = alive_jobs[: args.limit]

    count_alive_jobs = len(alive_jobs)
    if count_alive_jobs == 0:
        raise SystemExit("No alive jobs found.")
    if count_alive_jobs > 1 and not args.allow_multiple:
        raise SystemExit(f"Found {count_alive_jobs} alive jobs. Expected only one.")
    if count_alive_jobs == 1:
        rich.print("Found one alive job.")
    else:
        rich.print(f"Found {count_alive_jobs} alive jobs.")
