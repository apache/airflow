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

import sys
import time
from multiprocessing.pool import ApplyResult
from typing import List

from airflow_breeze.utils.console import console


def print_async_summary(completed_list: List[ApplyResult]) -> None:
    """
    Print summary of completed async results.
    :param completed_list: list of completed async results.
    """
    completed_list.sort(key=lambda x: x.get()[1])
    console.print()
    for result in completed_list:
        return_code, info = result.get()
        if return_code != 0:
            console.print(f"[red]NOK[/] for {info}: Return code: {return_code}.")
        else:
            console.print(f"[green]OK [/] for {info}.")
    console.print()


def get_completed_result_list(results: List[ApplyResult]) -> List[ApplyResult]:
    """Return completed results from the list."""
    return list(filter(lambda result: result.ready(), results))


def check_async_run_results(results: List[ApplyResult], poll_time: float = 0.2):
    """
    Check if all async results were success. Exits with error if not.
    :param results: results of parallel runs (expected in the form of Tuple: (return_code, info)
    :param poll_time: what's the poll time between checks
    """
    completed_number = 0
    total_number_of_results = len(results)
    completed_list = get_completed_result_list(results)
    while not len(completed_list) == total_number_of_results:
        current_completed_number = len(completed_list)
        if current_completed_number != completed_number:
            completed_number = current_completed_number
            console.print(
                f"\n[bright_blue]Completed {completed_number} out of {total_number_of_results} "
                f"({int(100*completed_number/total_number_of_results)}%).[/]\n"
            )
            print_async_summary(completed_list)
        time.sleep(poll_time)
        completed_list = get_completed_result_list(results)
    completed_number = len(completed_list)
    console.print(
        f"\n[bright_blue]Completed {completed_number} out of {total_number_of_results} "
        f"({int(100*completed_number/total_number_of_results)}%).[/]\n"
    )
    print_async_summary(completed_list)
    errors = False
    for result in results:
        if result.get()[0] != 0:
            errors = True
    if errors:
        console.print("\n[red]There were errors when running some tasks. Quitting.[/]\n")
        sys.exit(1)
    else:
        console.print("\n[green]All images are OK.[/]\n")
