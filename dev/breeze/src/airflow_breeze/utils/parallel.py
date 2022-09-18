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

import datetime
import os
import re
import sys
import textwrap
import time
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from multiprocessing.pool import ApplyResult, Pool
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Event, Thread
from typing import Generator, NamedTuple

from airflow_breeze.utils.console import MessageType, Output, get_console


def create_pool(parallelism: int) -> Pool:
    return Pool(parallelism)


def get_temp_file_name() -> str:
    file = NamedTemporaryFile(mode="w+t", delete=False, prefix="parallel")
    name = file.name
    file.close()
    return name


def get_output_files(titles: list[str]) -> list[Output]:
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


def get_last_lines_of_file(file_name: str, num_lines: int = 2) -> tuple[list[str], list[str]]:
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


class ProgressLines(NamedTuple):
    lines: list[str]
    skip_truncation: list[bool]


class AbstractProgressInfoMatcher(metaclass=ABCMeta):
    @abstractmethod
    def get_best_matching_lines(self, output: Output) -> ProgressLines | None:
        """
        Return best matching lines of the output. It also indicates if the lines potentially need truncation
        :param output: file that should be analysed for the output
        :return: tuple of array of lines to print and boolean indications whether the lines need truncation
        """


class DockerBuildxProgressMatcher(AbstractProgressInfoMatcher):
    DOCKER_BUILDX_PROGRESS_MATCHER = re.compile(r'\s*#(\d*) ')

    def __init__(self):
        self.last_docker_build_line: str | None = None

    def get_best_matching_lines(self, output) -> ProgressLines | None:
        last_lines, last_lines_no_colors = get_last_lines_of_file(output.file_name, num_lines=5)
        best_progress: int = 0
        best_line: str | None = None
        for index, line in enumerate(last_lines_no_colors):
            match = DockerBuildxProgressMatcher.DOCKER_BUILDX_PROGRESS_MATCHER.match(line)
            if match:
                docker_progress = int(match.group(1))
                if docker_progress > best_progress:
                    best_progress = docker_progress
                    best_line = last_lines[index]
        if best_line is None:
            best_line = self.last_docker_build_line
        else:
            self.last_docker_build_line = best_line
        if best_line is None:
            return None
        return ProgressLines(lines=[best_line], skip_truncation=[False])


class GenericRegexpProgressMatcher(AbstractProgressInfoMatcher):
    def __init__(
        self,
        regexp: str,
        lines_to_search: int,
        regexp_for_joined_line: str | None = None,
        regexp_to_skip_truncation: str | None = None,
    ):
        self.last_good_match: str | None = None
        self.matcher = re.compile(regexp)
        self.lines_to_search = lines_to_search
        self.matcher_for_joined_line = re.compile(regexp_for_joined_line) if regexp_for_joined_line else None
        self.matcher_to_skip_truncation = (
            re.compile(regexp_to_skip_truncation) if regexp_to_skip_truncation else None
        )

    def get_best_matching_lines(self, output: Output) -> ProgressLines | None:
        last_lines, last_lines_no_colors = get_last_lines_of_file(
            output.file_name, num_lines=self.lines_to_search
        )
        best_line: str | None = None
        previous_line: str | None = None
        for index, line in enumerate(last_lines_no_colors):
            match = self.matcher.match(line)
            if match:
                best_line = last_lines[index]
                if self.matcher_for_joined_line is not None and index > 0:
                    if self.matcher_for_joined_line.match(last_lines_no_colors[index - 1]):
                        previous_line = last_lines[index - 1].strip()
        if best_line is not None:
            if self.matcher_for_joined_line is not None and previous_line is not None:
                list_to_return: list[str] = [previous_line, best_line]
                skip_truncation: list[bool] = [
                    bool(self.matcher_to_skip_truncation.match(line))
                    if self.matcher_to_skip_truncation
                    else False
                    for line in list_to_return
                ]
                return ProgressLines(lines=list_to_return, skip_truncation=skip_truncation)
            else:
                self.last_good_match = best_line
        if self.last_good_match is None:
            return None
        return ProgressLines(
            lines=[self.last_good_match],
            skip_truncation=[
                bool(self.matcher_to_skip_truncation.match(self.last_good_match))
                if self.matcher_to_skip_truncation
                else False
            ],
        )


DOCKER_PULL_PROGRESS_REGEXP = r'^[0-9a-f]+: .*|.*\[[ 0-9]+%].*|^Waiting'


class ParallelMonitor(Thread):
    def __init__(
        self,
        outputs: list[Output],
        initial_time_in_seconds: int = 2,
        time_in_seconds: int = 10,
        progress_matcher: AbstractProgressInfoMatcher | None = None,
    ):
        super().__init__()
        self.outputs = outputs
        self.initial_time_in_seconds = initial_time_in_seconds
        self.time_in_seconds = time_in_seconds
        self.exit_event = Event()
        self.progress_matcher = progress_matcher
        self.start_time = datetime.datetime.utcnow()
        self.last_custom_progress: ProgressLines | None = None

    def print_single_progress(self, output: Output):
        if self.progress_matcher:
            custom_progress: ProgressLines | None = self.progress_matcher.get_best_matching_lines(output)
            custom_progress = self.last_custom_progress if custom_progress is None else custom_progress
            if custom_progress is not None:
                first_line = True
                for index, line in enumerate(custom_progress.lines):
                    if not custom_progress.skip_truncation[index]:
                        # Clear color just in case color reset is removed by textwrap.shorten
                        current_line = textwrap.shorten(custom_progress.lines[index], 155) + "\033[0;0m"
                    else:
                        current_line = custom_progress.lines[index]
                    if current_line:
                        prefix = f"Progress: {output.title:<30}"
                        if not first_line:
                            # remove job prefix after first line
                            prefix = " " * len(prefix)
                        print(f"{prefix}{current_line}\033[0;0m")
                        first_line = False
            else:
                size = os.path.getsize(output.file_name) if Path(output.file_name).exists() else 0
                print(f"Progress: {output.title:<30} {size:>153} bytes")

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
            self.exit_event.wait(self.initial_time_in_seconds)
            while not self.exit_event.is_set():
                self.print_summary()
                self.exit_event.wait(self.time_in_seconds)
        except Exception:
            get_console().print_exception(show_locals=True)


def print_async_summary(completed_list: list[ApplyResult]) -> None:
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


def get_completed_result_list(results: list[ApplyResult]) -> list[ApplyResult]:
    """Return completed results from the list."""
    return list(filter(lambda result: result.ready(), results))


def check_async_run_results(
    results: list[ApplyResult],
    success: str,
    outputs: list[Output],
    include_success_outputs: bool,
    poll_time: float = 0.2,
    skip_cleanup: bool = False,
):
    """
    Check if all async results were success. Exits with error if not.
    :param results: results of parallel runs (expected in the form of Tuple: (return_code, info)
    :param outputs: outputs where results are written to
    :param success: Success string printed when everything is OK
    :param include_success_outputs: include outputs of successful parallel runs
    :param poll_time: what's the poll time between checks
    :param skip_cleanup: whether to skip cleanup of temporary files.
    """
    from airflow_breeze.utils.ci_group import ci_group

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
        if message_type == MessageType.ERROR or include_success_outputs:
            with ci_group(title=f"{outputs[i].title}", message_type=message_type):
                os.write(1, Path(outputs[i].file_name).read_bytes())
        else:
            get_console().print(f"[success]{outputs[i].title}")
    try:
        if errors:
            get_console().print("\n[error]There were errors when running some tasks. Quitting.[/]\n")
            sys.exit(1)
        else:
            get_console().print(f"\n[success]{success}[/]\n")
    finally:
        if not skip_cleanup:
            for output in outputs:
                try:
                    os.unlink(output.file_name)
                except FileNotFoundError:
                    pass


@contextmanager
def run_with_pool(
    parallelism: int,
    all_params: list[str],
    initial_time_in_seconds: int = 2,
    time_in_seconds: int = 10,
    progress_matcher: AbstractProgressInfoMatcher | None = None,
) -> Generator[tuple[Pool, list[Output]], None, None]:
    get_console().print(f"Running with parallelism: {parallelism}")
    pool = create_pool(parallelism)
    outputs = get_output_files(all_params)
    m = ParallelMonitor(
        outputs=outputs,
        initial_time_in_seconds=initial_time_in_seconds,
        time_in_seconds=time_in_seconds,
        progress_matcher=progress_matcher,
    )
    m.start()
    yield pool, outputs
    pool.close()
    pool.join()
    m.cancel()
