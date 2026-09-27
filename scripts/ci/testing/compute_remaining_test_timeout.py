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

import argparse
import time

# Reserved so the test timeout handler can stop the containers and dump their logs, and the job
# still has time to upload those logs as artifacts, before GitHub cancels the job.
TEARDOWN_GRACE_SECONDS = 5 * 60

# Floor applied when the steps before the tests already ate the budget - the job is doomed either
# way, but the tests still get a chance to report something rather than none at all.
MINIMUM_TEST_TIMEOUT_SECONDS = 10 * 60


def compute_remaining_test_timeout(job_timeout_seconds: int, elapsed_seconds: int) -> int:
    """Return how long tests may run so their own timeout fires before the job timeout does."""
    remaining_seconds = job_timeout_seconds - elapsed_seconds - TEARDOWN_GRACE_SECONDS
    return max(remaining_seconds, MINIMUM_TEST_TIMEOUT_SECONDS)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print the number of seconds the tests may run within the remaining job budget."
    )
    parser.add_argument(
        "--job-timeout-minutes", type=int, required=True, help="The job's `timeout-minutes` value"
    )
    parser.add_argument(
        "--job-start-epoch", type=int, required=True, help="Unix timestamp stamped when the job started"
    )
    args = parser.parse_args()
    # Wall clock rather than time.monotonic(): the start timestamp comes from an earlier step in a
    # different process, so only an absolute clock is comparable across the two.
    elapsed_seconds = int(time.time()) - args.job_start_epoch
    print(compute_remaining_test_timeout(args.job_timeout_minutes * 60, elapsed_seconds))


if __name__ == "__main__":
    main()
