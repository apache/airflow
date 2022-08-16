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
import datetime
import os
import re
import signal
import sys
import time
from contextlib import contextmanager
from multiprocessing.pool import ApplyResult, Pool
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Event, Thread
from typing import Callable, Dict, Generator, List, Optional, Tuple

from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.console import MessageType, Output, get_console


def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def create_pool(parallelism: int) -> Pool:
    # we need to ignore SIGINT handling in workers in order to handle Ctrl-C nicely (with Pool's terminate)
    return Pool(parallelism, initializer=init_worker())


def get_temp_file_name() -> str:
    file = NamedTemporaryFile(mode="w+t", delete=False)
    name = file.name
    file.close()
    return name


def get_output_files(titles: List[str]) -> List[Output]:
    outputs = [Output(title=titles[i], file_name=get_temp_file_name()) for i in range(len(titles))]
    for out in outputs:
        get_console().print(f"[info]Capturing output of {out.title}:[/] {out.file_name}")
    return outputs


def nice_timedelta(delta: datetime.timedelta):
    d = {'d': delta.days}
    d['h'], rem = divmod(delta.seconds, 3600)
    d['m'], d['s'] = divmod(rem, 60)
    return "{d} days {h:02}:{m:02}:{s:02}".format(**d) if d['d'] else "{h:02}:{m:02}:{s:02}".format(**d)


ANSI_COLOUR_MATCHER = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')


def remove_ansi_colours(line):
    return ANSI_COLOUR_MATCHER.sub('', line)


def get_last_lines_of_file(file_name: str, num_lines: int = 2) -> Tuple[List[str], List[str]]:
    """
    Get last lines of a file efficiently, without reading the whole file (with some limitations).
    Assumptions ara that line length not bigger than ~180 chars.

    :param file_name: name of the file
    :param num_lines: number of lines to return (max)
    :return: Tuple - last lines of the file in two variants: original and with removed ansi colours
    """
    # account for EOL
    max_read = (180 + 2) * num_lines
    seek_size = min(os.stat(file_name).st_size, max_read)
    with open(file_name, 'rb') as temp_f:
        temp_f.seek(-seek_size, os.SEEK_END)
        tail = temp_f.read().decode(errors="ignore")
    last_lines = tail.splitlines()[-num_lines:]
    last_lines_no_colors = [remove_ansi_colours(line) for line in last_lines]
    return last_lines, last_lines_no_colors


DOCKER_BUILDX_PROGRESS_MATCHER = re.compile(r'\s*#(\d*) ')
last_docker_build_lines: Dict[Output, str] = {}


def progress_method_docker_buildx(output: Output) -> Optional[str]:
    last_lines, last_lines_no_colors = get_last_lines_of_file(output.file_name, num_lines=5)
    best_progress: int = 0
    best_line: Optional[str] = None
    for index, line in enumerate(last_lines_no_colors):
        match = DOCKER_BUILDX_PROGRESS_MATCHER.match(line)
        if match:
            docker_progress = int(match.group(1))
            if docker_progress > best_progress:
                best_progress = docker_progress
                best_line = last_lines[index]
    if best_line is None:
        best_line = last_docker_build_lines.get(output)
    else:
        last_docker_build_lines[output] = best_line
    return f"Progress: {output.title:<20} {best_line}"


DOCKER_PULL_PROGRESS_MATCHER = re.compile(r'^[0-9a-f]+: .*|.*\[[ 0-9]+%].*')
last_docker_pull_lines: Dict[Output, str] = {}


def progress_method_docker_pull(output: Output) -> Optional[str]:
    last_lines, last_lines_no_colors = get_last_lines_of_file(output.file_name, num_lines=15)
    best_line: Optional[str] = None
    for index, line in enumerate(last_lines_no_colors):
        match = DOCKER_PULL_PROGRESS_MATCHER.match(line)
        if match:
            best_line = last_lines[index]
    if best_line is None:
        best_line = last_docker_pull_lines.get(output)
    else:
        last_docker_pull_lines[output] = best_line
    return f"Progress: {output.title:<20} {best_line}"


class ParallelMonitor(Thread):
    def __init__(
        self,
        outputs: List[Output],
        time_in_seconds: int = 10,
        progress_method: Optional[Callable[[Output], Optional[str]]] = None,
    ):
        super().__init__()
        self.outputs = outputs
        self.time_in_seconds = time_in_seconds
        self.exit_event = Event()
        self.progress_method = progress_method
        self.start_time = datetime.datetime.utcnow()
        self.last_custom_progress: Optional[str] = None

    def print_single_progress(self, output: Output):
        if self.progress_method:
            progress = self.progress_method(output)
            if not progress and self.last_custom_progress:
                progress = self.last_custom_progress
            if progress:
                print(self.progress_method(output))
                return
        size = os.path.getsize(output.file_name) if Path(output.file_name).exists() else 0
        print(f"Progress: {output.title:<70} {size:>93} bytes")

    def print_summary(self):
        time_passed = datetime.datetime.utcnow() - self.start_time
        get_console().rule()
        for output in self.outputs:
            self.print_single_progress(output)
        get_console().rule(title=f"Time passed: {nice_timedelta(time_passed)}")

    def cancel(self):
        get_console().print("[info]Finishing progress monitoring.")
        self.exit_event.set()

    def run(self):
        try:
            while not self.exit_event.is_set():
                self.print_summary()
                self.exit_event.wait(self.time_in_seconds)
        except Exception:
            get_console().print_exception(show_locals=True)


def print_async_summary(completed_list: List[ApplyResult]) -> None:
    """
    Print summary of completed async results.
    :param completed_list: list of completed async results.
    """
    completed_list.sort(key=lambda x: x.get()[1])
    get_console().print()
    for result in completed_list:
        return_code, info = result.get()
        if return_code != 0:
            get_console().print(f"[error]NOK[/] for {info}: Return code: {return_code}.")
        else:
            get_console().print(f"[success]OK [/] for {info}.")
    get_console().print()


def get_completed_result_list(results: List[ApplyResult]) -> List[ApplyResult]:
    """Return completed results from the list."""
    return list(filter(lambda result: result.ready(), results))


def check_async_run_results(results: List[ApplyResult], outputs: List[Output], poll_time: float = 0.2):
    """
    Check if all async results were success. Exits with error if not.
    :param results: results of parallel runs (expected in the form of Tuple: (return_code, info)
    :param outputs: outputs where results are written to
    :param poll_time: what's the poll time between checks
    """
    completed_number = 0
    total_number_of_results = len(results)
    completed_list = get_completed_result_list(results)
    while not len(completed_list) == total_number_of_results:
        current_completed_number = len(completed_list)
        if current_completed_number != completed_number:
            completed_number = current_completed_number
            get_console().print(
                f"\n[info]Completed {completed_number} out of {total_number_of_results} "
                f"({int(100*completed_number/total_number_of_results)}%).[/]\n"
            )
            print_async_summary(completed_list)
        time.sleep(poll_time)
        completed_list = get_completed_result_list(results)
    completed_number = len(completed_list)
    get_console().print(
        f"\n[info]Completed {completed_number} out of {total_number_of_results} "
        f"({int(100*completed_number/total_number_of_results)}%).[/]\n"
    )
    print_async_summary(completed_list)
    errors = False
    for i, result in enumerate(results):
        if result.get()[0] != 0:
            errors = True
            message_type = MessageType.ERROR
        else:
            message_type = MessageType.SUCCESS
        with ci_group(title=f"{outputs[i].title}", message_type=message_type):
            os.write(1, Path(outputs[i].file_name).read_bytes())

    if errors:
        get_console().print("\n[error]There were errors when running some tasks. Quitting.[/]\n")
        sys.exit(1)
    else:
        get_console().print("\n[success]All images are OK.[/]\n")


@contextmanager
def run_with_pool(
    parallelism: int,
    all_params: List[str],
    time_in_seconds: int = 10,
    progress_method: Optional[Callable[[Output], Optional[str]]] = None,
) -> Generator[Tuple[Pool, List[Output]], None, None]:
    get_console().print(f"Parallelism: {parallelism}")
    pool = create_pool(parallelism)
    outputs = get_output_files(all_params)
    m = ParallelMonitor(outputs=outputs, time_in_seconds=time_in_seconds, progress_method=progress_method)
    m.start()
    yield pool, outputs
    pool.close()
    pool.join()
    m.cancel()
