# daemon/__init__.py
# Part of ‘python-daemon’, an implementation of PEP 3143.
#
# This is free software, and you are welcome to redistribute it under
# certain conditions; see the end of this file for copyright
# information, grant of license, and disclaimer of warranty.

""" Library to implement a well-behaved Unix daemon process.

    This library implements the well-behaved daemon specification of
    :pep:`3143`, “Standard daemon process library”.

    A well-behaved Unix daemon process is tricky to get right, but the
    required steps are much the same for every daemon program. A
    `DaemonContext` instance holds the behaviour and configured
    process environment for the program; use the instance as a context
    manager to enter a daemon state.

    Simple example of usage::

        import daemon

        from spam import do_main_program

        with daemon.DaemonContext():
            do_main_program()

    Customisation of the steps to become a daemon is available by
    setting options on the `DaemonContext` instance; see the
    documentation for that class for each option.
    """

from .daemon import DaemonContext


__all__ = ['DaemonContext']


# Copyright © 2009–2022 Ben Finney <ben+python@benfinney.id.au>
# Copyright © 2006 Robert Niederreiter
#
# This is free software: you may copy, modify, and/or distribute this work
# under the terms of the Apache License, version 2.0 as published by the
# Apache Software Foundation.
# No warranty expressed or implied. See the file ‘LICENSE.ASF-2’ for details.


# Local variables:
# coding: utf-8
# mode: python
# End:
# vim: fileencoding=utf-8 filetype=python :
