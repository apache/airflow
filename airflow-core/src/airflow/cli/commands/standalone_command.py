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

import logging
import os
import socket
import subprocess
import threading
import time
from collections import deque
from typing import TYPE_CHECKING

from termcolor import colored

from airflow.api_fastapi.app import create_auth_manager
from airflow.configuration import conf
from airflow.executors import executor_constants
from airflow.executors.executor_loader import ExecutorLoader
from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.job import most_recent_job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.utils import db
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

if TYPE_CHECKING:
    from airflow.jobs.base_job_runner import BaseJobRunner

FORCE_COLOR = bool(os.environ.get("FORCE_COLOR", ""))
NO_COLOR = bool(os.environ.get("NO_COLOR", ""))


class StandaloneCommand:
    """
    Runs all components of Airflow under a single parent process.

    Useful for local development.
    """

    @classmethod
    def entrypoint(cls, args):
        """CLI entrypoint, called by the main CLI system."""
        StandaloneCommand().run()

    def __init__(self):
        self.subcommands = {}
        self.output_queue = deque()
        self.ready_time = None
        self.ready_delay = 3

    @providers_configuration_loaded
    def run(self):
        self.print_output("standalone", "Starting Airflow Standalone")
        # Silence built-in logging at INFO
        logging.getLogger("").setLevel(logging.WARNING)
        # Startup checks and prep
        env = self.calculate_env()
        self.find_user_info()
        self.initialize_database()
        # Set up commands to run
        self.subcommands["scheduler"] = SubCommand(
            self,
            name="scheduler",
            command=["scheduler"],
            env=env,
        )
        self.subcommands["dag-processor"] = SubCommand(
            self,
            name="dag-processor",
            command=["dag-processor"],
            env=env,
        )
        self.subcommands["api-server"] = SubCommand(
            self,
            name="api-server",
            command=["api-server"],
            env=env,
        )
        self.subcommands["triggerer"] = SubCommand(
            self,
            name="triggerer",
            command=["triggerer"],
            env=env,
        )

        # Run subcommand threads
        for command in self.subcommands.values():
            command.start()
        # Run output loop
        shown_ready = False
        try:
            while True:
                # Print all the current lines onto the screen
                self.update_output()
                # Print info banner when all components are ready and the
                # delay has passed
                if not self.ready_time and self.is_ready():
                    self.ready_time = time.monotonic()
                if (
                    not shown_ready
                    and self.ready_time
                    and time.monotonic() - self.ready_time > self.ready_delay
                ):
                    self.print_ready()
                    shown_ready = True
                # Ensure we idle-sleep rather than fast-looping
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass
        # Stop subcommand threads
        self.print_output("standalone", "Shutting down components")
        for command in self.subcommands.values():
            command.stop()
        for command in self.subcommands.values():
            command.join()
        self.print_output("standalone", "Complete")

    def update_output(self):
        """Drains the output queue and prints its contents to the screen."""
        while self.output_queue:
            # Extract info
            name, line = self.output_queue.popleft()
            # Make line printable
            line_str = line.decode("utf8").strip()
            self.print_output(name, line_str)

    def print_output(self, name: str, output):
        """
        Print an output line with name and colouring.

        You can pass multiple lines to output if you wish; it will be split for you.
        """
        color: dict[str, str] = {
            "api-server": "magenta",
            "scheduler": "blue",
            "dag-processor": "yellow",
            "triggerer": "cyan",
            "standalone": "white",
        }
        colorised_name = colored(
            f"{name:10}", color.get(name, "white"), no_color=NO_COLOR, force_color=FORCE_COLOR
        )
        for line in output.splitlines():
            print(f"{colorised_name} | {line.strip()}")

    def print_error(self, name: str, output):
        """
        Print an error message to the console.

        This is the same as print_output but with the text red
        """
        self.print_output(name, colored(output, "red"))

    def calculate_env(self):
        """
        Works out the environment variables needed to run subprocesses.

        We override some settings as part of being standalone.
        """
        env = dict(os.environ)

        # Make sure we're using a local executor flavour
        executor_class, _ = ExecutorLoader.import_default_executor_cls()
        if not executor_class.is_local:
            self.print_output("standalone", "Forcing executor to LocalExecutor")
            env["AIRFLOW__CORE__EXECUTOR"] = executor_constants.LOCAL_EXECUTOR

        # Make sure we're using SimpleAuthManager
        simple_auth_manager_classpath = (
            "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
        )
        if conf.get("core", "auth_manager") != simple_auth_manager_classpath:
            self.print_output("standalone", "Forcing auth manager to SimpleAuthManager")
            env["AIRFLOW__CORE__AUTH_MANAGER"] = simple_auth_manager_classpath
            os.environ["AIRFLOW__CORE__AUTH_MANAGER"] = simple_auth_manager_classpath  # also in this process!

        return env

    def find_user_info(self):
        if conf.get("core", "simple_auth_manager_all_admins").lower() == "true":
            # If we have no auth anyways, no need to print or do anything
            return
        if conf.get("core", "simple_auth_manager_users") != "admin:admin":
            self.print_output(
                "standalone",
                "Not outputting user passwords - `[core] simple_auth_manager_users` is already set.",
            )
            return

        am = create_auth_manager()

        password_file = am.get_generated_password_file()
        if os.path.exists(password_file):
            self.print_output(
                "standalone",
                f"Password for the admin user has been previously generated in {password_file}. Not echoing it here.",
            )
            return

        # this generates the password and prints it
        am.init()

    def initialize_database(self):
        """Make sure all the tables are created."""
        # Set up DB tables
        self.print_output("standalone", "Checking database is initialized")
        db.initdb()
        self.print_output("standalone", "Database ready")

    def is_ready(self):
        """
        Detect when all Airflow components are ready to serve.

        For now, it's simply time-based.
        """
        return (
            self.job_running(SchedulerJobRunner)
            and self.job_running(DagProcessorJobRunner)
            and self.job_running(TriggererJobRunner)
        )

    def port_open(self, port):
        """
        Check if the given port is listening on the local machine.

        Used to tell if webserver is alive.
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            sock.connect(("127.0.0.1", port))
            sock.close()
        except (OSError, ValueError):
            # Any exception means the socket is not available
            return False
        return True

    def job_running(self, job_runner_class: type[BaseJobRunner]):
        """
        Check if the given job name is running and heartbeating correctly.

        Used to tell if a component is alive.
        """
        recent = most_recent_job(job_runner_class.job_type)
        if not recent:
            return False
        return recent.is_alive()

    def print_ready(self):
        """
        Print the banner shown when Airflow is ready to go.

        Include with login details.
        """
        self.print_output("standalone", "")
        self.print_output("standalone", "Airflow is ready")
        self.print_output(
            "standalone",
            "Airflow Standalone is for development purposes only. Do not use this in production!",
        )
        self.print_output("standalone", "")


class SubCommand(threading.Thread):
    """
    Execute a subcommand on another thread.

    Thread that launches a process and then streams its output back to the main
    command. We use threads to avoid using select() and raw filehandles, and the
    complex logic that brings doing line buffering.
    """

    def __init__(self, parent, name: str, command: list[str], env: dict[str, str]):
        super().__init__()
        self.parent = parent
        self.name = name
        self.command = command
        self.env = env

    def run(self):
        """Run the actual process and captures it output to a queue."""
        self.process = subprocess.Popen(
            ["airflow", *self.command],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=self.env,
        )
        for line in self.process.stdout:
            self.parent.output_queue.append((self.name, line))

    def stop(self):
        """Call to stop this process (and thus this thread)."""
        self.process.terminate()


# Alias for use in the CLI parser
standalone = StandaloneCommand.entrypoint
