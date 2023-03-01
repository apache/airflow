# daemon/pidfile.py
# Part of ‘python-daemon’, an implementation of PEP 3143.
#
# This is free software, and you are welcome to redistribute it under
# certain conditions; see the end of this file for copyright
# information, grant of license, and disclaimer of warranty.

""" Lockfile behaviour implemented via Unix PID files. """

from lockfile.pidlockfile import PIDLockFile


class TimeoutPIDLockFile(PIDLockFile, object):
    """ Lockfile with default timeout, implemented as a Unix PID file.

        This uses the ``PIDLockFile`` implementation, with the
        following changes:

        * The `acquire_timeout` parameter to the initialiser will be
          used as the default `timeout` parameter for the `acquire`
          method.
        """

    def __init__(self, path, acquire_timeout=None, *args, **kwargs):
        """ Set up the parameters of a TimeoutPIDLockFile.

            :param path: Filesystem path to the PID file.
            :param acquire_timeout: Value to use by default for the
                `acquire` call.
            :return: ``None``.
            """
        self.acquire_timeout = acquire_timeout
        super().__init__(path, *args, **kwargs)

    def acquire(self, timeout=None, *args, **kwargs):
        """ Acquire the lock.

            :param timeout: Specifies the timeout; see below for valid
                values.
            :return: ``None``.

            The `timeout` defaults to the value set during
            initialisation with the `acquire_timeout` parameter. It is
            passed to `PIDLockFile.acquire`; see that method for
            details.
            """
        if timeout is None:
            timeout = self.acquire_timeout
        super().acquire(timeout, *args, **kwargs)


# Copyright © 2008–2022 Ben Finney <ben+python@benfinney.id.au>
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
