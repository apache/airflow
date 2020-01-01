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

import errno
import os
import re
import signal
import subprocess
from datetime import datetime
from functools import reduce

import psutil
from jinja2 import Template

from airflow.configuration import conf
from airflow.exceptions import AirflowException

# When killing processes, time to wait after issuing a SIGTERM before issuing a
# SIGKILL.
DEFAULT_TIME_TO_WAIT_AFTER_SIGTERM = conf.getint(
    'core', 'KILLED_TASK_CLEANUP_TIME'
)

KEY_REGEX = re.compile(r'^[\w.-]+$')


def validate_key(k, max_length=250):
    if not isinstance(k, str):
        raise TypeError("The key has to be a string")
    elif len(k) > max_length:
        raise AirflowException(
            "The key has to be less than {0} characters".format(max_length))
    elif not KEY_REGEX.match(k):
        raise AirflowException(
            "The key ({k}) has to be made of alphanumeric characters, dashes, "
            "dots and underscores exclusively".format(k=k))
    else:
        return True


def alchemy_to_dict(obj):
    """
    Transforms a SQLAlchemy model instance into a dictionary
    """
    if not obj:
        return None
    d = {}
    for c in obj.__table__.columns:
        value = getattr(obj, c.name)
        if type(value) == datetime:
            value = value.isoformat()
        d[c.name] = value
    return d


def ask_yesno(question):
    yes = {'yes', 'y'}
    no = {'no', 'n'}

    done = False
    print(question)
    while not done:
        choice = input().lower()
        if choice in yes:
            return True
        elif choice in no:
            return False
        else:
            print("Please respond by yes or no.")


def is_container(obj):
    """
    Test if an object is a container (iterable) but not a string
    """
    return hasattr(obj, '__iter__') and not isinstance(obj, str)


def as_tuple(obj):
    """
    If obj is a container, returns obj as a tuple.
    Otherwise, returns a tuple containing obj.
    """
    if is_container(obj):
        return tuple(obj)
    else:
        return tuple([obj])


def chunks(items, chunk_size):
    """
    Yield successive chunks of a given size from a list of items
    """
    if chunk_size <= 0:
        raise ValueError('Chunk size must be a positive integer')
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def reduce_in_chunks(fn, iterable, initializer, chunk_size=0):
    """
    Reduce the given list of items by splitting it into chunks
    of the given size and passing each chunk through the reducer
    """
    if len(iterable) == 0:
        return initializer
    if chunk_size == 0:
        chunk_size = len(iterable)
    return reduce(fn, chunks(iterable, chunk_size), initializer)


def as_flattened_list(iterable):
    """
    Return an iterable with one level flattened

    >>> as_flattened_list((('blue', 'red'), ('green', 'yellow', 'pink')))
    ['blue', 'red', 'green', 'yellow', 'pink']
    """
    return [e for i in iterable for e in i]


def pprinttable(rows):
    """Returns a pretty ascii table from tuples

    If namedtuple are used, the table will have headers
    """
    if not rows:
        return
    if hasattr(rows[0], '_fields'):  # if namedtuple
        headers = rows[0]._fields
    else:
        headers = ["col{}".format(i) for i in range(len(rows[0]))]
    lens = [len(s) for s in headers]

    for row in rows:
        for i in range(len(rows[0])):
            slenght = len("{}".format(row[i]))
            if slenght > lens[i]:
                lens[i] = slenght
    formats = []
    hformats = []
    for i in range(len(rows[0])):
        if isinstance(rows[0][i], int):
            formats.append("%%%dd" % lens[i])
        else:
            formats.append("%%-%ds" % lens[i])
        hformats.append("%%-%ds" % lens[i])
    pattern = " | ".join(formats)
    hpattern = " | ".join(hformats)
    separator = "-+-".join(['-' * n for n in lens])
    s = ""
    s += separator + '\n'
    s += (hpattern % tuple(headers)) + '\n'
    s += separator + '\n'

    def f(t):
        return "{}".format(t) if isinstance(t, str) else t

    for line in rows:
        s += pattern % tuple(f(t) for t in line) + '\n'
    s += separator + '\n'
    return s


def reap_process_group(pgid, log, sig=signal.SIGTERM,
                       timeout=DEFAULT_TIME_TO_WAIT_AFTER_SIGTERM):
    """
    Tries really hard to terminate all processes in the group (including grandchildren). Will send
    sig (SIGTERM) to the process group of pid. If any process is alive after timeout
    a SIGKILL will be send.

    :param log: log handler
    :param pgid: process group id to kill
    :param sig: signal type
    :param timeout: how much time a process has to terminate
    """

    returncodes = {}

    def on_terminate(p):
        log.info("Process %s (%s) terminated with exit code %s", p, p.pid, p.returncode)
        returncodes[p.pid] = p.returncode

    def signal_procs(sig):
        try:
            os.killpg(pgid, sig)
        except OSError as err:
            # If operation not permitted error is thrown due to run_as_user,
            # use sudo -n(--non-interactive) to kill the process
            if err.errno == errno.EPERM:
                subprocess.check_call(
                    ["sudo", "-n", "kill", "-" + str(sig)] + map(children, lambda p: str(p.pid))
                )
            else:
                raise

    if pgid == os.getpgid(0):
        raise RuntimeError("I refuse to kill myself")

    try:
        parent = psutil.Process(pgid)

        children = parent.children(recursive=True)
        children.append(parent)
    except psutil.NoSuchProcess:
        # The process already exited, but maybe it's children haven't.
        children = []
        for p in psutil.process_iter():
            try:
                if os.getpgid(p.pid) == pgid and p.pid != 0:
                    children.append(p)
            except OSError:
                pass

    log.info("Sending %s to GPID %s", sig, pgid)
    try:
        signal_procs(sig)
    except OSError as err:
        # No such process, which means there is no such process group - our job
        # is done
        if err.errno == errno.ESRCH:
            return returncodes

    _, alive = psutil.wait_procs(children, timeout=timeout, callback=on_terminate)

    if alive:
        for p in alive:
            log.warning("process %s did not respond to SIGTERM. Trying SIGKILL", p)

        try:
            signal_procs(signal.SIGKILL)
        except OSError as err:
            if err.errno != errno.ESRCH:
                raise

        _, alive = psutil.wait_procs(alive, timeout=timeout, callback=on_terminate)
        if alive:
            for p in alive:
                log.error("Process %s (%s) could not be killed. Giving up.", p, p.pid)
    return returncodes


def parse_template_string(template_string):
    if "{{" in template_string:  # jinja mode
        return None, Template(template_string)
    else:
        return template_string, None


def render_log_filename(ti, try_number, filename_template):
    """
    Given task instance, try_number, filename_template, return the rendered log
    filename

    :param ti: task instance
    :param try_number: try_number of the task
    :param filename_template: filename template, which can be jinja template or
        python string template
    """
    filename_template, filename_jinja_template = parse_template_string(filename_template)
    if filename_jinja_template:
        jinja_context = ti.get_template_context()
        jinja_context['try_number'] = try_number
        return filename_jinja_template.render(**jinja_context)

    return filename_template.format(dag_id=ti.dag_id,
                                    task_id=ti.task_id,
                                    execution_date=ti.execution_date.isoformat(),
                                    try_number=try_number)


def convert_camel_to_snake(camel_str):
    return re.sub('(?!^)([A-Z]+)', r'_\1', camel_str).lower()
