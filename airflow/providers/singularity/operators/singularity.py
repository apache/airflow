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

import ast
import os
import shutil
from typing import TYPE_CHECKING, Any, Sequence

from spython.main import Client

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SingularityOperator(BaseOperator):
    """
    Execute a command inside a Singularity container

    Singularity has more seamless connection to the host than Docker, so
    no special binds are needed to ensure binding content in the user $HOME
    and temporary directories. If the user needs custom binds, this can
    be done with --volumes

    :param image: Singularity image or URI from which to create the container.
    :param auto_remove: Delete the container when the process exits.
        The default is False.
    :param command: Command to be run in the container. (templated)
    :param start_command: Start command to pass to the container instance.
    :param environment: Environment variables to set in the container. (templated)
    :param working_dir: Set a working directory for the instance.
    :param force_pull: Pull the image on every run. Default is False.
    :param volumes: List of volumes to mount into the container, e.g.
        ``['/host/path:/container/path', '/host/path2:/container/path2']``.
    :param options: Other flags (list) to provide to the instance start.
    :param working_dir: Working directory to
        set on the container (equivalent to the -w switch the docker client).
    """

    template_fields: Sequence[str] = (
        "command",
        "environment",
    )
    template_ext: Sequence[str] = (
        ".sh",
        ".bash",
    )
    template_fields_renderers = {"command": "bash", "environment": "json"}

    def __init__(
        self,
        *,
        image: str,
        command: str | ast.AST,
        start_command: str | list[str] | None = None,
        environment: dict[str, Any] | None = None,
        pull_folder: str | None = None,
        working_dir: str | None = None,
        force_pull: bool | None = False,
        volumes: list[str] | None = None,
        options: list[str] | None = None,
        auto_remove: bool | None = False,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.auto_remove = auto_remove
        self.command = command
        self.start_command = start_command
        self.environment = environment or {}
        self.force_pull = force_pull
        self.image = image
        self.instance = None
        self.options = options or []
        self.pull_folder = pull_folder
        self.volumes = volumes or []
        self.working_dir = working_dir
        self.cli = None
        self.container = None

    def execute(self, context: Context) -> None:

        self.log.info("Preparing Singularity container %s", self.image)
        self.cli = Client

        if not self.command:
            raise AirflowException("You must define a command.")

        # Pull the container if asked, and ensure not a binary file
        if self.force_pull and not os.path.exists(self.image):
            self.log.info("Pulling container %s", self.image)
            image = self.cli.pull(  # type: ignore[attr-defined]
                self.image, stream=True, pull_folder=self.pull_folder
            )

            # If we need to stream result for the user, returns lines
            if isinstance(image, list):
                lines = image.pop()
                image = image[0]
                for line in lines:
                    self.log.info(line)

            # Update the image to be a filepath on the system
            self.image = image

        # Prepare list of binds
        for bind in self.volumes:
            self.options += ["--bind", bind]

        # Does the user want a custom working directory?
        if self.working_dir is not None:
            self.options += ["--workdir", self.working_dir]

        # Export environment before instance is run
        for enkey, envar in self.environment.items():
            self.log.debug("Exporting %s=%s", envar, enkey)
            os.putenv(enkey, envar)
            os.environ[enkey] = envar

        # Create a container instance
        self.log.debug("Options include: %s", self.options)
        self.instance = self.cli.instance(  # type: ignore[attr-defined]
            self.image, options=self.options, args=self.start_command, start=False
        )

        self.instance.start()  # type: ignore[attr-defined]
        self.log.info(self.instance.cmd)  # type: ignore[attr-defined]
        self.log.info("Created instance %s from %s", self.instance, self.image)

        self.log.info("Running command %s", self._get_command())
        self.cli.quiet = True  # type: ignore[attr-defined]
        result = self.cli.execute(  # type: ignore[attr-defined]
            self.instance, self._get_command(), return_result=True
        )

        # Stop the instance
        self.log.info("Stopping instance %s", self.instance)
        self.instance.stop()  # type: ignore[attr-defined]

        if self.auto_remove is True:
            if self.auto_remove and os.path.exists(self.image):
                shutil.rmtree(self.image)

        # If the container failed, raise the exception
        if result["return_code"] != 0:
            message = result["message"]
            raise AirflowException(f"Singularity failed: {message}")

        self.log.info("Output from command %s", result["message"])

    def _get_command(self) -> Any | None:
        if self.command is not None and self.command.strip().find("[") == 0:  # type: ignore
            commands = ast.literal_eval(self.command)
        else:
            commands = self.command
        return commands

    def on_kill(self) -> None:
        if self.instance is not None:
            self.log.info("Stopping Singularity instance")
            self.instance.stop()

            # If an image exists, clean it up
            if self.auto_remove is True:
                if self.auto_remove and os.path.exists(self.image):
                    shutil.rmtree(self.image)
