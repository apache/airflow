# -*- coding: utf-8 -*-
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
"""
Class responsible for colouring logs based on log level.
"""
import sys
from typing import Any, Union

from logging import LogRecord
from colorlog import TTYColoredFormatter
from termcolor import colored

ARGS = {"attrs": ["bold"]}

DEFAULT_COLORS = {
    "DEBUG": "red",
    "INFO": "",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "red",
}


class CustomTTYColoredFormatter(TTYColoredFormatter):
    """
    Custom log formatter which extends `colored.TTYColoredFormatter`
    by adding attributes to message arguments and coloring error
    traceback.
    """
    def __init__(self, *args, **kwargs):
        kwargs["stream"] = sys.stdout or kwargs.get("stream")
        kwargs["log_colors"] = DEFAULT_COLORS
        super().__init__(*args, **kwargs)

    @staticmethod
    def _color_arg(arg: Any) -> Union[str, float, int]:
        if isinstance(arg, (int, float)):
            # In case of %d or %f formatting
            return arg
        return colored(str(arg), **ARGS)  # type: ignore

    def _color_record_args(self, record: LogRecord) -> LogRecord:
        if isinstance(record.args, (tuple, list)):
            record.args = tuple(self._color_arg(arg) for arg in record.args)
        elif isinstance(record.args, dict):
            # Case of logging.debug("a %(a)d b %(b)s", {'a':1, 'b':2})
            record.args = {
                key: self._color_arg(value) for key, value in record.args.items()
            }
        elif isinstance(record.args, str):
            record.args = self._color_arg(record.args)
        return record

    def _color_record_traceback(self, record: LogRecord) -> LogRecord:
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)

            if record.exc_text:
                record.exc_text = colored(record.exc_text, DEFAULT_COLORS["ERROR"])
        return record

    def format(self, record: LogRecord) -> str:
        record = self._color_record_args(record)
        record = self._color_record_traceback(record)
        return super().format(record)
