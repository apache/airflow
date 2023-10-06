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

import shutil
from typing import TYPE_CHECKING

from airflow.io.fsspec import FsspecFileIO
from airflow.models import BaseOperator

if TYPE_CHECKING:
    from airflow.io import InputFile, OutputFile
    from airflow.utils.context import Context


class FileTransfer(BaseOperator):
    """
    Copies a file from a source to a destination.

    This streams the file from the source to the destination, so it does not
    need to fit into memory.

    :param src: The source file path or InputFile object.
    :param dst: The destination file path or OutputFile object.
    :param source_conn_id: The optional source connection id.
    :param dest_conn_id: The optional destination connection id.
    """

    def __init__(
        self,
        *,
        src: str | InputFile,
        dst: str | OutputFile,
        source_conn_id: str | None,
        dest_conn_id: str | None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.src = src
        self.dst = dst
        self.source_conn_id = source_conn_id
        self.dst_conn_id = dest_conn_id

    def execute(self, context: Context) -> None:
        src: InputFile
        dst: OutputFile

        fileio = FsspecFileIO()
        if isinstance(self.src, str):
            src = fileio.new_input(self.src, self.source_conn_id)
        else:
            src = self.src

        if isinstance(self.dst, str):
            dst = fileio.new_output(self.dst, self.dst_conn_id)
        else:
            dst = self.dst

        with src.open() as s, dst.create() as d:
            shutil.copyfileobj(s, d)
