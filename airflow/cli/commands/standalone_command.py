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
import random
import socket
import subprocess
import threading
import time
from collections import deque

from termcolor import colored

from airflow.configuration import AIRFLOW_HOME, conf
from airflow.executors import executor_constants
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.jobs.triggerer_job import TriggererJob
from airflow.utils import db
from airflow.www.app import cached_app


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
        self.user_info = {}
        self.ready_time = None
        self.ready_delay = 3

    def run(self):
        """Main run loop"""
        self.print_output("standalone", "Starting Airflow Standalone")
        # Silence built-in logging at INFO
        logging.getLogger("").setLevel(logging.WARNING)
        # Startup checks and prep
        env = self.calculate_env()
        self.initialize_database()
        # Set up commands to run
        self.subcommands["scheduler"] = SubCommand(
            self,
            name="scheduler",
            command=["scheduler"],
            env=env,
        )
        self.subcommands["webserver"] = SubCommand(
            self,
            name="webserver",
            command=["webserver"],
            env=env,
        )
        self.subcommands["triggerer"] = SubCommand(
            self,
            name="triggerer",
            command=["triggerer"],
            env=env,
        )

        self.web_server_port = conf.getint('webserver', 'WEB_SERVER_PORT', fallback=8080)
        # Run subcommand threads
        for command in self.subcommands.values():
            command.start()
        # Run output loop
        shown_ready = False
        while True:
            try:
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
                break
        # Stop subcommand threads
        self.print_output("standalone", "Shutting down components")
        for command in self.subcommands.values():
            command.stop()
        for command in self.subcommands.values():
            command.join()
        self.print_output("standalone", "Complete")

    def update_output(self):
        """Drains the output queue and prints its contents to the screen"""
        while self.output_queue:
            # Extract info
            name, line = self.output_queue.popleft()
            # Make line printable
            line_str = line.decode("utf8").strip()
            self.print_output(name, line_str)

    def print_output(self, name: str, output):
        """
        Prints an output line with name and colouring. You can pass multiple
        lines to output if you wish; it will be split for you.
        """
        color = {
            "webserver": "green",
            "scheduler": "blue",
            "triggerer": "cyan",
            "standalone": "white",
        }.get(name, "white")
        colorised_name = colored("%10s" % name, color)
        for line in output.split("\n"):
            print(f"{colorised_name} | {line.strip()}")

    def print_error(self, name: str, output):
        """
        Prints an error message to the console (this is the same as
        print_output but with the text red)
        """
        self.print_output(name, colored(output, "red"))

    def calculate_env(self):
        """
        Works out the environment variables needed to run subprocesses.
        We override some settings as part of being standalone.
        """
        env = dict(os.environ)
        # Make sure we're using a local executor flavour
        if conf.get("core", "executor") not in [
            executor_constants.LOCAL_EXECUTOR,
            executor_constants.SEQUENTIAL_EXECUTOR,
        ]:
            if "sqlite" in conf.get("database", "sql_alchemy_conn"):
                self.print_output("standalone", "Forcing executor to SequentialExecutor")
                env["AIRFLOW__CORE__EXECUTOR"] = executor_constants.SEQUENTIAL_EXECUTOR
            else:
                self.print_output("standalone", "Forcing executor to LocalExecutor")
                env["AIRFLOW__CORE__EXECUTOR"] = executor_constants.LOCAL_EXECUTOR
        return env

    def initialize_database(self):
        """Makes sure all the tables are created."""
        # Set up DB tables
        self.print_output("standalone", "Checking database is initialized")
        db.initdb()
        self.print_output("standalone", "Database ready")
        # See if a user needs creating
        # We want a streamlined first-run experience, but we do not want to
        # use a preset password as people will inevitably run this on a public
        # server. Thus, we make a random password and store it in AIRFLOW_HOME,
        # with the reasoning that if you can read that directory, you can see
        # the database credentials anyway.
        appbuilder = cached_app().appbuilder
        user_exists = appbuilder.sm.find_user("admin")
        password_path = os.path.join(AIRFLOW_HOME, "standalone_admin_password.txt")
        we_know_password = os.path.isfile(password_path)
        # If the user does not exist, make a random password and make it
        if not user_exists:
            self.print_output("standalone", "Creating admin user")
            role = appbuilder.sm.find_role("Admin")
            assert role is not None
            password = "".join(
                random.choice("abcdefghkmnpqrstuvwxyzABCDEFGHKMNPQRSTUVWXYZ23456789") for i in range(16)
            )
            with open(password_path, "w") as file:
                file.write(password)
            appbuilder.sm.add_user("admin", "Admin", "User", "admin@example.com", role, password)
            self.print_output("standalone", "Created admin user")
        # If the user does exist and we know its password, read the password
        elif user_exists and we_know_password:
            with open(password_path) as file:
                password = file.read().strip()
        # Otherwise we don't know the password
        else:
            password = None
        # Store what we know about the user for printing later in startup
        self.user_info = {"username": "admin", "password": password}

    def is_ready(self):
        """
        Detects when all Airflow components are ready to serve.
        For now, it's simply time-based.
        """
        return (
            self.port_open(self.web_server_port)
            and self.job_running(SchedulerJob)
            and self.job_running(TriggererJob)
        )

    def port_open(self, port):
        """
        Checks if the given port is listening on the local machine.
        (used to tell if webserver is alive)
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

    def job_running(self, job):
        """
        Checks if the given job name is running and heartbeating correctly
        (used to tell if scheduler is alive)
        """
        recent = job.most_recent_job()
        if not recent:
            return False
        return recent.is_alive()

    def print_ready(self):
        """
        Prints the banner shown when Airflow is ready to go, with login
        details.
        """
        self.print_output("standalone", "")
        self.print_output("standalone", "Airflow is ready")
        if self.user_info["password"]:
            self.print_output(
                "standalone",
                f"Login with username: {self.user_info['username']}  password: {self.user_info['password']}",
            )
        self.print_output(
            "standalone",
            "Airflow Standalone is for development purposes only. Do not use this in production!",
        )
        self.print_output("standalone", "")


class SubCommand(threading.Thread):
    """
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
        """Runs the actual process and captures it output to a queue"""
        self.process = subprocess.Popen(
            ["airflow"] + self.command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=self.env,
        )
        for line in self.process.stdout:
            self.parent.output_queue.append((self.name, line))

    def stop(self):
        """Call to stop this process (and thus this thread)"""
        self.process.terminate()


# Alias for use in the CLI parser
standalone = StandaloneCommand.entrypoint
