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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import errno
import imp
import sys
import warnings

import psutil

from builtins import input
from past.builtins import basestring
from datetime import datetime
from functools import reduce
import os
import re
import signal

from jinja2 import Template
from zope.deprecation import deprecate

from airflow import configuration
from airflow.exceptions import AirflowException

# When killing processes, time to wait after issuing a SIGTERM before issuing a
# SIGKILL.
DEFAULT_TIME_TO_WAIT_AFTER_SIGTERM = configuration.conf.getint(
    'core', 'KILLED_TASK_CLEANUP_TIME'
)


def validate_key(k, max_length=250):
    if not isinstance(k, basestring):
        raise TypeError("The key has to be a string")
    elif len(k) > max_length:
        raise AirflowException(
            "The key has to be less than {0} characters".format(max_length))
    elif not re.match(r'^[A-Za-z0-9_\-\.]+$', k):
        raise AirflowException(
            "The key ({k}) has to be made of alphanumeric characters, dashes, "
            "dots and underscores exclusively".format(**locals()))
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
    yes = set(['yes', 'y'])
    no = set(['no', 'n'])

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


def is_in(obj, l):
    """
    Checks whether an object is one of the item in the list.
    This is different from ``in`` because ``in`` uses __cmp__ when
    present. Here we change based on the object itself
    """
    for item in l:
        if item is obj:
            return True
    return False


def is_container(obj):
    """
    Test if an object is a container (iterable) but not a string
    """
    return hasattr(obj, '__iter__') and not isinstance(obj, basestring)


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


@deprecate("chain will be removed in Airflow 2.0")
def chain(*tasks):
    """
    Given a number of tasks, builds a dependency chain.

    chain(task_1, task_2, task_3, task_4)

    is equivalent to

    task_1.set_downstream(task_2)
    task_2.set_downstream(task_3)
    task_3.set_downstream(task_4)
    """
    for up_task, down_task in zip(tasks[:-1], tasks[1:]):
        up_task.set_downstream(down_task)


def cross_downstream(from_tasks, to_tasks):
    r"""
    Set downstream dependencies for all tasks in from_tasks to all tasks in to_tasks.
    E.g.: cross_downstream(from_tasks=[t1, t2, t3], to_tasks=[t4, t5, t6])
    Is equivalent to:

    t1 --> t4
       \ /
    t2 -X> t5
       / \
    t3 --> t6

    t1.set_downstream(t4)
    t1.set_downstream(t5)
    t1.set_downstream(t6)
    t2.set_downstream(t4)
    t2.set_downstream(t5)
    t2.set_downstream(t6)
    t3.set_downstream(t4)
    t3.set_downstream(t5)
    t3.set_downstream(t6)

    :param from_tasks: List of tasks to start from.
    :type from_tasks: List[airflow.models.BaseOperator]
    :param to_tasks: List of tasks to set as downstream dependencies.
    :type to_tasks: List[airflow.models.BaseOperator]
    """
    for task in from_tasks:
        task.set_downstream(to_tasks)


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
        return "{}".format(t) if isinstance(t, basestring) else t

    for line in rows:
        s += pattern % tuple(f(t) for t in line) + '\n'
    s += separator + '\n'
    return s


def reap_process_group(pid, log, sig=signal.SIGTERM,
                       timeout=DEFAULT_TIME_TO_WAIT_AFTER_SIGTERM):
    """
    Tries really hard to terminate all children (including grandchildren). Will send
    sig (SIGTERM) to the process group of pid. If any process is alive after timeout
    a SIGKILL will be send.

    :param log: log handler
    :param pid: pid to kill
    :param sig: signal type
    :param timeout: how much time a process has to terminate
    """

    def on_terminate(p):
        log.info("Process %s (%s) terminated with exit code %s", p, p.pid, p.returncode)

    if pid == os.getpid():
        raise RuntimeError("I refuse to kill myself")

    parent = psutil.Process(pid)

    children = parent.children(recursive=True)
    children.append(parent)

    try:
        pg = os.getpgid(pid)
    except OSError as err:
        # Skip if not such process - we experience a race and it just terminated
        if err.errno == errno.ESRCH:
            return
        raise

    log.info("Sending %s to GPID %s", sig, pg)
    os.killpg(os.getpgid(pid), sig)

    gone, alive = psutil.wait_procs(children, timeout=timeout, callback=on_terminate)

    if alive:
        for p in alive:
            log.warn("process %s (%s) did not respond to SIGTERM. Trying SIGKILL", p, pid)

        os.killpg(os.getpgid(pid), signal.SIGKILL)

        gone, alive = psutil.wait_procs(alive, timeout=timeout, callback=on_terminate)
        if alive:
            for p in alive:
                log.error("Process %s (%s) could not be killed. Giving up.", p, p.pid)


def parse_template_string(template_string):
    if "{{" in template_string:  # jinja mode
        return None, Template(template_string)
    else:
        return template_string, None


class AirflowImporter(object):
    """
    Importer that dynamically loads a class and module from its parent. This
    allows Airflow to support ``from airflow.operators import BashOperator``
    even though BashOperator is actually in
    ``airflow.operators.bash_operator``.

    The importer also takes over for the parent_module by wrapping it. This is
    required to support attribute-based usage:

    .. code:: python

        from airflow import operators
        operators.BashOperator(...)
    """

    def __init__(self, parent_module, module_attributes):
        """
        :param parent_module: The string package name of the parent module. For
            example, 'airflow.operators'
        :type parent_module: str
        :param module_attributes: The file to class mappings for all importable
            classes.
        :type module_attributes: str
        """
        self._parent_module = parent_module
        self._attribute_modules = self._build_attribute_modules(module_attributes)
        self._loaded_modules = {}

        # Wrap the module so we can take over __getattr__.
        sys.modules[parent_module.__name__] = self

    @staticmethod
    def _build_attribute_modules(module_attributes):
        """
        Flips and flattens the module_attributes dictionary from:

            module => [Attribute, ...]

        To:

            Attribute => module

        This is useful so that we can find the module to use, given an
        attribute.
        """
        attribute_modules = {}

        for module, attributes in list(module_attributes.items()):
            for attribute in attributes:
                attribute_modules[attribute] = module

        return attribute_modules

    def _load_attribute(self, attribute):
        """
        Load the class attribute if it hasn't been loaded yet, and return it.
        """
        module = self._attribute_modules.get(attribute, False)

        if not module:
            # This shouldn't happen. The check happens in find_modules, too.
            raise ImportError(attribute)
        elif module not in self._loaded_modules:
            # Note that it's very important to only load a given modules once.
            # If they are loaded more than once, the memory reference to the
            # class objects changes, and Python thinks that an object of type
            # Foo that was declared before Foo's module was reloaded is no
            # longer the same type as Foo after it's reloaded.
            path = os.path.realpath(self._parent_module.__file__)
            folder = os.path.dirname(path)
            f, filename, description = imp.find_module(module, [folder])
            self._loaded_modules[module] = imp.load_module(module, f, filename, description)

            # This functionality is deprecated, and AirflowImporter should be
            # removed in 2.0.
            warnings.warn(
                "Importing '{i}' directly from '{m}' has been "
                "deprecated. Please import from "
                "'{m}.[operator_module]' instead. Support for direct "
                "imports will be dropped entirely in Airflow 2.0.".format(
                    i=attribute, m=self._parent_module.__name__),
                DeprecationWarning)

        loaded_module = self._loaded_modules[module]

        return getattr(loaded_module, attribute)

    def __getattr__(self, attribute):
        """
        Get an attribute from the wrapped module. If the attribute doesn't
        exist, try and import it as a class from a submodule.

        This is a Python trick that allows the class to pretend it's a module,
        so that attribute-based usage works:

            from airflow import operators
            operators.BashOperator(...)

        It also allows normal from imports to work:

            from airflow.operators.bash_operator import BashOperator
        """
        if hasattr(self._parent_module, attribute):
            # Always default to the parent module if the attribute exists.
            return getattr(self._parent_module, attribute)
        elif attribute in self._attribute_modules:
            # Try and import the attribute if it's got a module defined.
            loaded_attribute = self._load_attribute(attribute)
            setattr(self, attribute, loaded_attribute)
            return loaded_attribute

        raise AttributeError


def render_log_filename(ti, try_number, filename_template):
    """
    Given task instance, try_number, filename_template, return the rendered log filename

    :param ti: task instance
    :param try_number: try_number of the task
    :param filename_template: filename template, which can be jinja template or python string template
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
