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
#
"""
Utilities module for cli
"""
from __future__ import absolute_import
from __future__ import print_function

import functools
import getpass
import json
import os
import socket
import struct
import sys
from argparse import Namespace
from datetime import datetime
from fcntl import ioctl
from termios import TIOCGWINSZ

from airflow.models import Log
from airflow.utils import cli_action_loggers
from airflow.utils.platform import is_terminal_support_colors


def action_logging(f):
    """
    Decorates function to execute function at the same time submitting action_logging
    but in CLI context. It will call action logger callbacks twice,
    one for pre-execution and the other one for post-execution.

    Action logger will be called with below keyword parameters:
        sub_command : name of sub-command
        start_datetime : start datetime instance by utc
        end_datetime : end datetime instance by utc
        full_command : full command line arguments
        user : current user
        log : airflow.models.log.Log ORM instance
        dag_id : dag id (optional)
        task_id : task_id (optional)
        execution_date : execution date (optional)
        error : exception instance if there's an exception

    :param f: function instance
    :return: wrapped function
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        """
        An wrapper for cli functions. It assumes to have Namespace instance
        at 1st positional argument

        :param args: Positional argument. It assumes to have Namespace instance
            at 1st positional argument
        :param kwargs: A passthrough keyword argument
        """
        assert args
        assert isinstance(args[0], Namespace), \
            "1st positional argument should be argparse.Namespace instance, " \
            "but {}".format(args[0])
        metrics = _build_metrics(f.__name__, args[0])
        cli_action_loggers.on_pre_execution(**metrics)
        try:
            return f(*args, **kwargs)
        except Exception as e:
            metrics['error'] = e
            raise
        finally:
            metrics['end_datetime'] = datetime.utcnow()
            cli_action_loggers.on_post_execution(**metrics)

    return wrapper


def _build_metrics(func_name, namespace):
    """
    Builds metrics dict from function args
    It assumes that function arguments is from airflow.bin.cli module's function
    and has Namespace instance where it optionally contains "dag_id", "task_id",
    and "execution_date".

    :param func_name: name of function
    :param namespace: Namespace instance from argparse
    :return: dict with metrics
    """
    sensitive_fields = {'-p', '--password', '--conn-password'}
    full_command = list(sys.argv)
    for idx, command in enumerate(full_command):  # pylint: disable=too-many-nested-blocks
        if command in sensitive_fields:
            # For cases when password is passed as "--password xyz" (with space between key and value)
            full_command[idx + 1] = "*" * 8
        else:
            # For cases when password is passed as "--password=xyz" (with '=' between key and value)
            for sensitive_field in sensitive_fields:
                if command.startswith('{}='.format(sensitive_field)):
                    full_command[idx] = '{}={}'.format(sensitive_field, "*" * 8)

    metrics = {'sub_command': func_name, 'start_datetime': datetime.utcnow(),
               'full_command': '{}'.format(full_command), 'user': getpass.getuser()}

    assert isinstance(namespace, Namespace)
    tmp_dic = vars(namespace)
    metrics['dag_id'] = tmp_dic.get('dag_id')
    metrics['task_id'] = tmp_dic.get('task_id')
    metrics['execution_date'] = tmp_dic.get('execution_date')
    metrics['host_name'] = socket.gethostname()

    extra = json.dumps(dict((k, metrics[k]) for k in ('host_name', 'full_command')))
    log = Log(
        event='cli_{}'.format(func_name),
        task_instance=None,
        owner=metrics['user'],
        extra=extra,
        task_id=metrics.get('task_id'),
        dag_id=metrics.get('dag_id'),
        execution_date=metrics.get('execution_date'))
    metrics['log'] = log
    return metrics


class ColorMode:
    """
    Coloring modes. If `auto` is then automatically detected.
    """
    ON = "on"
    OFF = "off"
    AUTO = "auto"


def should_use_colors(args):
    """
    Processes arguments and decides whether to enable color in output
    """
    if args.color == ColorMode.ON:
        return True
    if args.color == ColorMode.OFF:
        return False
    return is_terminal_support_colors()


def get_terminal_size(fallback=(80, 20)):
    """Return a tuple of (terminal height, terminal width)."""
    try:
        return struct.unpack('hhhh', ioctl(sys.__stdout__, TIOCGWINSZ, '\000' * 8))[0:2]
    except IOError:
        # when the output stream or init descriptor is not a tty, such
        # as when when stdout is piped to another program
        pass
    try:
        return int(os.environ.get('LINES')), int(os.environ.get('COLUMNS'))
    except TypeError:
        return fallback


def header(text, fillchar):
    rows, columns = get_terminal_size()
    print(" {} ".format(text).center(columns, fillchar))


def deprecated_action(func=None, new_name=None, sub_commands=False):
    if not func:
        return functools.partial(deprecated_action, new_name=new_name, sub_commands=sub_commands)

    stream = sys.stderr
    try:
        from pip._vendor import colorama
        WINDOWS = (sys.platform.startswith("win") or
                   (sys.platform == 'cli' and os.name == 'nt'))
        if WINDOWS:
            stream = colorama.AnsiToWin32(sys.stderr)
    except Exception:
        colorama = None

    def should_color():
        # Don't colorize things if we do not have colorama or if told not to
        if not colorama:
            return False

        real_stream = (
            stream if not isinstance(stream, colorama.AnsiToWin32)
            else stream.wrapped
        )

        # If the stream is a tty we should color it
        if hasattr(real_stream, "isatty") and real_stream.isatty():
            return True

        if os.environ.get("TERM") and "color" in os.environ.get("TERM"):
            return True

        # If anything else we should not color it
        return False

    @functools.wraps(func)
    def wrapper(args):
        if getattr(args, 'deprecation_warning', True):
            command = args.subcommand or args.func.__name__
            if sub_commands:
                msg = (
                    "The mode (-l, -d, etc) options to {!r} have been deprecated and removed in Airflow 2.0,"
                    " please use the get/set/list subcommands instead"
                ).format(command)
            else:
                prefix = "The {!r} command is deprecated and removed in Airflow 2.0, please use "
                if isinstance(new_name, list):
                    msg = prefix.format(args.subcommand)
                    new_names = list(map(repr, new_name))
                    msg += "{}, or {}".format(", ".join(new_names[:-1]), new_names[-1])
                    msg += " instead"
                else:
                    msg = (prefix + "{!r} instead").format(command, new_name)

            if should_color():
                msg = "".join([colorama.Fore.YELLOW, msg, colorama.Style.RESET_ALL])
            print(msg, file=sys.stderr)
        func(args)
    return wrapper
