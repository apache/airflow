# daemon/daemon.py
# Part of ‘python-daemon’, an implementation of PEP 3143.
#
# This is free software, and you are welcome to redistribute it under
# certain conditions; see the end of this file for copyright
# information, grant of license, and disclaimer of warranty.

""" Daemon process behaviour. """

import atexit
import errno
import os
import pwd
import resource
import signal
import socket
import sys
import warnings


class DaemonError(Exception):
    """ Base exception class for errors from this module. """


class DaemonOSEnvironmentError(DaemonError, OSError):
    """ Exception raised when daemon OS environment setup receives error. """


class DaemonProcessDetachError(DaemonError, OSError):
    """ Exception raised when process detach fails. """


class DaemonContext:
    """ Context for turning the current program into a daemon process.

        A `DaemonContext` instance represents the behaviour settings and
        process context for the program when it becomes a daemon. The
        behaviour and environment is customised by setting options on the
        instance, before calling the `open` method.

        Each option can be passed as a keyword argument to the `DaemonContext`
        constructor, or subsequently altered by assigning to an attribute on
        the instance at any time prior to calling `open`. That is, for
        options named `wibble` and `wubble`, the following invocation::

            foo = daemon.DaemonContext(wibble=bar, wubble=baz)
            foo.open()

        is equivalent to::

            foo = daemon.DaemonContext()
            foo.wibble = bar
            foo.wubble = baz
            foo.open()

        The following options are defined.

        `files_preserve`
            :Default: ``None``

            List of files that should *not* be closed when starting the
            daemon. If ``None``, all open file descriptors will be closed.

            Elements of the list are file descriptors (as returned by a file
            object's `fileno()` method) or Python `file` objects. Each
            specifies a file that is not to be closed during daemon start.

        `chroot_directory`
            :Default: ``None``

            Full path to a directory to set as the effective root directory of
            the process. If ``None``, specifies that the root directory is not
            to be changed.

        `working_directory`
            :Default: ``'/'``

            Full path of the working directory to which the process should
            change on daemon start.

            Since a filesystem cannot be unmounted if a process has its
            current working directory on that filesystem, this should either
            be left at default or set to a directory that is a sensible “home
            directory” for the daemon while it is running.

        `umask`
            :Default: ``0``

            File access creation mask (“umask”) to set for the process on
            daemon start.

            A daemon should not rely on the parent process's umask value,
            which is beyond its control and may prevent creating a file with
            the required access mode. So when the daemon context opens, the
            umask is set to an explicit known value.

            If the conventional value of 0 is too open, consider setting a
            value such as 0o022, 0o027, 0o077, or another specific value.
            Otherwise, ensure the daemon creates every file with an
            explicit access mode for the purpose.

        `pidfile`
            :Default: ``None``

            Context manager for a PID lock file. When the daemon context opens
            and closes, it enters and exits the `pidfile` context manager.

        `detach_process`
            :Default: ``None``

            If ``True``, detach the process context when opening the daemon
            context; if ``False``, do not detach.

            If unspecified (``None``) during initialisation of the instance,
            this will be set to ``True`` by default, and ``False`` only if
            detaching the process is determined to be redundant; for example,
            in the case when the process was started by `init`, by `initd`, or
            by `inetd`.

        `signal_map`
            :Default: system-dependent

            Mapping from operating system signals to callback actions.

            The mapping is used when the daemon context opens, and determines
            the action for each signal's signal handler:

            * A value of ``None`` will ignore the signal (by setting the
              signal action to ``signal.SIG_IGN``).

            * A string value will be used as the name of an attribute on the
              ``DaemonContext`` instance. The attribute's value will be used
              as the action for the signal handler.

            * Any other value will be used as the action for the
              signal handler. See the ``signal.signal`` documentation
              for details of the signal handler interface.

            The default value depends on which signals are defined on the
            running system. Each item from the list below whose signal is
            actually defined in the ``signal`` module will appear in the
            default map:

            * ``signal.SIGTTIN``: ``None``

            * ``signal.SIGTTOU``: ``None``

            * ``signal.SIGTSTP``: ``None``

            * ``signal.SIGTERM``: ``'terminate'``

            Depending on how the program will interact with its child
            processes, it may need to specify a signal map that
            includes the ``signal.SIGCHLD`` signal (received when a
            child process exits). See the specific operating system's
            documentation for more detail on how to determine what
            circumstances dictate the need for signal handlers.

        `uid`
            :Default: ``os.getuid()``

        `gid`
            :Default: ``os.getgid()``

            The user ID (“UID”) value and group ID (“GID”) value to switch
            the process to on daemon start.

            The default values, the real UID and GID of the process, will
            relinquish any effective privilege elevation inherited by the
            process.

        `initgroups`
            :Default: ``False``

            If true, set the daemon process's supplementary groups as
            determined by the specified `uid`.

            This will require that the current process UID has
            permission to change the process's owning GID.

        `prevent_core`
            :Default: ``True``

            If true, prevents the generation of core files, in order to avoid
            leaking sensitive information from daemons run as `root`.

        `stdin`
            :Default: ``None``

        `stdout`
            :Default: ``None``

        `stderr`
            :Default: ``None``

            Each of `stdin`, `stdout`, and `stderr` is a file-like object
            which will be used as the new file for the standard I/O stream
            `sys.stdin`, `sys.stdout`, and `sys.stderr` respectively. The file
            should therefore be open, with a minimum of mode 'r' in the case
            of `stdin`, and mimimum of mode 'w+' in the case of `stdout` and
            `stderr`.

            If the object has a `fileno()` method that returns a file
            descriptor, the corresponding file will be excluded from being
            closed during daemon start (that is, it will be treated as though
            it were listed in `files_preserve`).

            If ``None``, the corresponding system stream is re-bound to the
            file named by `os.devnull`.
        """

    def __init__(
            self,
            chroot_directory=None,
            working_directory="/",
            umask=0,
            uid=None,
            gid=None,
            initgroups=False,
            prevent_core=True,
            detach_process=None,
            files_preserve=None,
            pidfile=None,
            stdin=None,
            stdout=None,
            stderr=None,
            signal_map=None,
            ):
        """ Set up a new instance. """
        self.chroot_directory = chroot_directory
        self.working_directory = working_directory
        self.umask = umask
        self.prevent_core = prevent_core
        self.files_preserve = files_preserve
        self.pidfile = pidfile
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr

        if uid is None:
            uid = os.getuid()
        self.uid = uid
        if gid is None:
            gid = os.getgid()
        self.gid = gid
        self.initgroups = initgroups

        if detach_process is None:
            detach_process = is_detach_process_context_required()
        self.detach_process = detach_process

        if signal_map is None:
            signal_map = make_default_signal_map()
        self.signal_map = signal_map

        self._is_open = False

    @property
    def is_open(self):
        """ ``True`` if the instance is currently open. """
        return self._is_open

    def open(self):
        """ Become a daemon process.

            :return: ``None``.

            Open the daemon context, turning the current program into a daemon
            process. This performs the following steps:

            * If this instance's `is_open` property is true, return
              immediately. This makes it safe to call `open` multiple times on
              an instance.

            * If the `prevent_core` attribute is true, set the resource limits
              for the process to prevent any core dump from the process.

            * If the `chroot_directory` attribute is not ``None``, set the
              effective root directory of the process to that directory (via
              `os.chroot`).

              This allows running the daemon process inside a “chroot gaol”
              as a means of limiting the system's exposure to rogue behaviour
              by the process. Note that the specified directory needs to
              already be set up for this purpose.

            * Set the process owner (UID and GID) to the `uid` and `gid`
              attribute values.

              If the `initgroups` attribute is true, also set the process's
              supplementary groups to all the user's groups (i.e. those
              groups whose membership includes the username corresponding
              to `uid`).

            * Close all open file descriptors. This excludes those listed in
              the `files_preserve` attribute, and those that correspond to the
              `stdin`, `stdout`, or `stderr` attributes.

            * Change current working directory to the path specified by the
              `working_directory` attribute.

            * Reset the file access creation mask to the value specified by
              the `umask` attribute.

            * If the `detach_process` option is true, detach the current
              process into its own process group, and disassociate from any
              controlling terminal.

            * Set signal handlers as specified by the `signal_map` attribute.

            * If any of the attributes `stdin`, `stdout`, `stderr` are not
              ``None``, bind the system streams `sys.stdin`, `sys.stdout`,
              and/or `sys.stderr` to the files represented by the
              corresponding attributes. Where the attribute has a file
              descriptor, the descriptor is duplicated (instead of re-binding
              the name).

            * If the `pidfile` attribute is not ``None``, enter its context
              manager.

            * Mark this instance as open (for the purpose of future `open` and
              `close` calls).

            * Register the `close` method to be called during Python's exit
              processing.

            When the function returns, the running program is a daemon
            process.
            """
        if self.is_open:
            return

        if self.chroot_directory is not None:
            change_root_directory(self.chroot_directory)

        if self.prevent_core:
            prevent_core_dump()

        change_file_creation_mask(self.umask)
        change_working_directory(self.working_directory)
        change_process_owner(self.uid, self.gid, self.initgroups)

        if self.detach_process:
            detach_process_context()

        signal_handler_map = self._make_signal_handler_map()
        set_signal_handlers(signal_handler_map)

        exclude_fds = self._get_exclude_file_descriptors()
        close_all_open_files(exclude=exclude_fds)

        redirect_stream(sys.stdin, self.stdin)
        redirect_stream(sys.stdout, self.stdout)
        redirect_stream(sys.stderr, self.stderr)

        if self.pidfile is not None:
            self.pidfile.__enter__()

        self._is_open = True

        register_atexit_function(self.close)

    def __enter__(self):
        """ Context manager entry point. """
        self.open()
        return self

    def close(self):
        """ Exit the daemon process context.

            :return: ``None``.

            Close the daemon context. This performs the following steps:

            * If this instance's `is_open` property is false, return
              immediately. This makes it safe to call `close` multiple times
              on an instance.

            * If the `pidfile` attribute is not ``None``, exit its context
              manager.

            * Mark this instance as closed (for the purpose of future `open`
              and `close` calls).
            """
        if not self.is_open:
            return

        if self.pidfile is not None:
            # Follow the interface for telling a context manager to exit,
            # <URL:https://docs.python.org/3/library/stdtypes.html#typecontextmanager>.
            self.pidfile.__exit__(None, None, None)

        self._is_open = False

    def __exit__(self, exc_type, exc_value, traceback):
        """ Context manager exit point. """
        self.close()

    def terminate(self, signal_number, stack_frame):
        """ Signal handler for end-process signals.

            :param signal_number: The OS signal number received.
            :param stack_frame: The frame object at the point the
                signal was received.
            :return: ``None``.

            Signal handler for the ``signal.SIGTERM`` signal. Performs the
            following step:

            * Raise a ``SystemExit`` exception explaining the signal.
            """
        exception = SystemExit(
                "Terminating on signal {signal_number!r}".format(
                    signal_number=signal_number))
        raise exception

    def _get_exclude_file_descriptors(self):
        """ Get the set of file descriptors to exclude closing.

            :return: A set containing the file descriptors for the
                files to be preserved.

            The file descriptors to be preserved are those from the
            items in `files_preserve`, and also each of `stdin`,
            `stdout`, and `stderr`. For each item:

            * If the item is ``None``, omit it from the return set.

            * If the item's `fileno` method returns a value, include
              that value in the return set.

            * Otherwise, include the item verbatim in the return set.
            """
        files_preserve = self.files_preserve
        if files_preserve is None:
            files_preserve = []
        files_preserve.extend(
                item for item in {self.stdin, self.stdout, self.stderr}
                if hasattr(item, 'fileno'))

        exclude_descriptors = set()
        for item in files_preserve:
            if item is None:
                continue
            file_descriptor = _get_file_descriptor(item)
            if file_descriptor is not None:
                exclude_descriptors.add(file_descriptor)
            else:
                exclude_descriptors.add(item)

        return exclude_descriptors

    def _make_signal_handler(self, target):
        """ Make the signal handler for a specified target object.

            :param target: A specification of the target for the
                handler; see below.
            :return: The value for use by `signal.signal()`.

            If `target` is ``None``, return ``signal.SIG_IGN``. If `target`
            is a text string, return the attribute of this instance named
            by that string. Otherwise, return `target` itself.
            """
        if target is None:
            result = signal.SIG_IGN
        elif isinstance(target, str):
            name = target
            result = getattr(self, name)
        else:
            result = target

        return result

    def _make_signal_handler_map(self):
        """ Make the map from signals to handlers for this instance.

            :return: The constructed signal map for this instance.

            Construct a map from signal numbers to handlers for this
            context instance, suitable for passing to
            `set_signal_handlers`.
            """
        signal_handler_map = dict(
                (signal_number, self._make_signal_handler(target))
                for (signal_number, target) in self.signal_map.items())
        return signal_handler_map


def get_stream_file_descriptors(
        stdin=sys.stdin,
        stdout=sys.stdout,
        stderr=sys.stderr,
        ):
    """ Get the set of file descriptors for the process streams.

        :stdin: The input stream for the process (default:
            `sys.stdin`).
        :stdout: The ouput stream for the process (default:
            `sys.stdout`).
        :stderr: The diagnostic stream for the process (default:
            `sys.stderr`).
        :return: A `set` of each file descriptor (integer) for the
            streams.

        The standard streams are the files `sys.stdin`, `sys.stdout`,
        `sys.stderr`.

        Streams might in some circumstances be non-file objects.
        Include in the result only those streams that actually have a
        file descriptor (as returned by the `fileno` method).
        """
    file_descriptors = set(
            fd for fd in set(
                _get_file_descriptor(stream)
                for stream in {stdin, stdout, stderr})
            if fd is not None)
    return file_descriptors


def _get_file_descriptor(obj):
    """ Get the file descriptor, if the object has one.

        :param obj: The object expected to be a file-like object.
        :return: The file descriptor iff the file supports it; otherwise
            ``None``.

        The object may be a non-file object. It may also be a
        file-like object with no support for a file descriptor. In
        either case, return ``None``.
        """
    file_descriptor = None
    if hasattr(obj, 'fileno'):
        try:
            file_descriptor = obj.fileno()
        except ValueError:
            # The item doesn't support a file descriptor.
            pass

    return file_descriptor


def change_working_directory(directory):
    """ Change the working directory of this process.

        :param directory: The target directory path.
        :return: ``None``.
        """
    try:
        os.chdir(directory)
    except Exception as exc:
        error = DaemonOSEnvironmentError(
                "Unable to change working directory ({exc})".format(exc=exc))
        raise error from exc


def change_root_directory(directory):
    """ Change the root directory of this process.

        :param directory: The target directory path.
        :return: ``None``.

        Set the current working directory, then the process root directory,
        to the specified `directory`. Requires appropriate OS privileges
        for this process.
        """
    try:
        os.chdir(directory)
        os.chroot(directory)
    except Exception as exc:
        error = DaemonOSEnvironmentError(
                "Unable to change root directory ({exc})".format(exc=exc))
        raise error from exc


def change_file_creation_mask(mask):
    """ Change the file creation mask for this process.

        :param mask: The numeric file creation mask to set.
        :return: ``None``.
        """
    try:
        os.umask(mask)
    except Exception as exc:
        error = DaemonOSEnvironmentError(
                "Unable to change file creation mask ({exc})".format(exc=exc))
        raise error from exc


def get_username_for_uid(uid):
    """ Get the username for the specified UID. """
    passwd_entry = pwd.getpwuid(uid)
    username = passwd_entry.pw_name

    return username


def change_process_owner(uid, gid, initgroups=False):
    """ Change the owning UID, GID, and groups of this process.

        :param uid: The target UID for the daemon process.
        :param gid: The target GID for the daemon process.
        :param initgroups: If true, initialise the supplementary
            groups of the process.
        :return: ``None``.

        Sets the owning GID and UID of the process (in that order, to
        avoid permission errors) to the specified `gid` and `uid`
        values.

        If `initgroups` is true, the supplementary groups of the
        process are also initialised, with those corresponding to the
        username for the target UID.

        All these operations require appropriate OS privileges. If
        permission is denied, a ``DaemonOSEnvironmentError`` is
        raised.
        """
    try:
        username = get_username_for_uid(uid)
    except KeyError:
        # We don't have a username to pass to ‘os.initgroups’.
        initgroups = False

    try:
        if initgroups:
            os.initgroups(username, gid)
        else:
            os.setgid(gid)
        os.setuid(uid)
    except Exception as exc:
        error = DaemonOSEnvironmentError(
                "Unable to change process owner ({exc})".format(exc=exc))
        raise error from exc


def prevent_core_dump():
    """ Prevent this process from generating a core dump.

        :return: ``None``.

        Set the soft and hard limits for core dump size to zero. On Unix,
        this entirely prevents the process from creating core dump.
        """
    core_resource = resource.RLIMIT_CORE

    try:
        # Ensure the resource limit exists on this platform, by requesting
        # its current value.
        resource.getrlimit(core_resource)
    except ValueError as exc:
        error = DaemonOSEnvironmentError(
                "System does not support RLIMIT_CORE resource limit"
                " ({exc})".format(exc=exc))
        raise error from exc

    # Set hard and soft limits to zero, i.e. no core dump at all.
    core_limit = (0, 0)
    resource.setrlimit(core_resource, core_limit)


def detach_process_context():
    """ Detach the process context from parent and session.

        :return: ``None``.

        Detach from the parent process and session group, allowing the
        parent to exit while this process continues running.

        Reference: “Advanced Programming in the Unix Environment”,
        section 13.3, by W. Richard Stevens, published 1993 by
        Addison-Wesley.
        """

    def fork_then_exit_parent(error_message):
        """ Fork a child process, then exit the parent process.

            :param error_message: Message for the exception in case of a
                detach failure.
            :return: ``None``.
            :raise DaemonProcessDetachError: If the fork fails.
            """
        try:
            pid = os.fork()
            if pid > 0:
                os._exit(0)
        except OSError as exc:
            error = DaemonProcessDetachError(
                    "{message}: [{exc.errno:d}] {exc.strerror}".format(
                        message=error_message, exc=exc))
            raise error from exc

    fork_then_exit_parent(error_message="Failed first fork")
    os.setsid()
    fork_then_exit_parent(error_message="Failed second fork")


def is_process_started_by_init():
    """ Determine whether the current process is started by `init`.

        :return: ``True`` iff the parent process is `init`; otherwise
            ``False``.

        The `init` process is the one with process ID of 1.
        """
    result = False

    init_pid = 1
    if os.getppid() == init_pid:
        result = True

    return result


def is_socket(fd):
    """ Determine whether the file descriptor is a socket.

        :param fd: The file descriptor to interrogate.
        :return: ``True`` iff the file descriptor is a socket; otherwise
            ``False``.

        Query the socket type of `fd`. If there is no error, the file is a
        socket.
        """
    warnings.warn(
            DeprecationWarning("migrate to `is_socket_file` instead"))

    result = False

    try:
        file_socket = socket.fromfd(fd, socket.AF_INET, socket.SOCK_RAW)
        file_socket.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE)
    except socket.error as exc:
        exc_errno = exc.args[0]
        if exc_errno == errno.ENOTSOCK:
            # Socket operation on non-socket.
            pass
        else:
            # Some other socket error.
            result = True
    else:
        # No error getting socket type.
        result = True

    return result


def is_socket_file(file):
    """ Determine whether the `file` is a socket.

        :param file: The file (an `io.IOBase` instance) to interrogate.
        :return: ``True`` iff `file` is a socket; otherwise ``False``.

        Query the socket type of the file descriptor of `file`. If there is no
        error, the file is a socket.
        """
    result = False

    try:
        file_fd = file.fileno()
    except ValueError:
        # The file doesn't have a file descriptor.
        file_fd = None

    try:
        file_socket = socket.fromfd(file_fd, socket.AF_INET, socket.SOCK_RAW)
        file_socket.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE)
    except socket.error as exc:
        exc_errno = exc.args[0]
        if exc_errno == errno.ENOTSOCK:
            # Socket operation on non-socket.
            pass
        else:
            # Some other socket error.
            result = True
    else:
        # No error getting socket type.
        result = True

    return result


def is_process_started_by_superserver():
    """ Determine whether the current process is started by the superserver.

        :return: ``True`` if this process was started by the internet
            superserver; otherwise ``False``.

        The internet superserver creates a network socket, and
        attaches it to the standard streams of the child process. If
        that is the case for this process, return ``True``, otherwise
        ``False``.
        """
    result = False

    if is_socket_file(sys.__stdin__):
        result = True

    return result


def is_detach_process_context_required():
    """ Determine whether detaching the process context is required.

        :return: ``False`` iff the process is already detached;
            otherwise ``True``.

        The process environment is interrogated for the following:

        * Process was started by `init`; or

        * Process was started by `inetd`.

        If any of the above are true, the process is deemed to be already
        detached.
        """
    result = True
    if is_process_started_by_init() or is_process_started_by_superserver():
        result = False

    return result


def close_file_descriptor_if_open(fd):
    """ Close a file descriptor if already open.

        :param fd: The file descriptor to close.
        :return: ``None``.

        Close the file descriptor `fd`, suppressing an error in the
        case the file was not open.
        """
    try:
        os.close(fd)
    except EnvironmentError as exc:
        if exc.errno == errno.EBADF:
            # File descriptor was not open.
            pass
        else:
            error = DaemonOSEnvironmentError(
                    "Failed to close file descriptor {fd:d} ({exc})".format(
                        fd=fd, exc=exc))
            raise error from exc


MAXFD = 2048


def get_maximum_file_descriptors():
    """ Get the maximum number of open file descriptors for this process.

        :return: The number (integer) to use as the maximum number of open
            files for this process.

        The maximum is the process hard resource limit of maximum number of
        open file descriptors. If the limit is “infinity”, a default value
        of ``MAXFD`` is returned.
        """
    (__, hard_limit) = resource.getrlimit(resource.RLIMIT_NOFILE)

    result = hard_limit
    if hard_limit == resource.RLIM_INFINITY:
        result = MAXFD

    return result


_total_file_descriptor_range = range(0, get_maximum_file_descriptors())


def _validate_fd_values(fds):
    """ Validate the collection of file descriptors `fds`.

        :param fds: A collection of file descriptors.
        :raise TypeError: When any of the `fds` are an invalid type.
        :return: ``None``.

        A valid file descriptor is an `int` value.
        """
    invalid_fds = set(filter((lambda fd: not isinstance(fd, int)), fds))
    if invalid_fds:
        value_to_complain_about = next(invalid_fds)
        raise TypeError(
                "not an integer file descriptor", value_to_complain_about)


def _get_candidate_file_descriptor_ranges(exclude):
    """ Get the collection of candidate file descriptor ranges.

        :param exclude: A collection of file descriptors that should
            be excluded from the return ranges.
        :return: The collection (a `list`) of ranges that contain the
            file descriptors that are candidates for files that may be
            open in this process.

        Determine the ranges of all the candidate file descriptors.
        Each range is a pair of `int` values (`low`, `high`).

        A value is a candidate if it could be an open file descriptor
        in this process, excluding those integers in the `exclude`
        collection.
        """
    _validate_fd_values(exclude)
    ranges = []

    remaining_range = _total_file_descriptor_range
    for exclude_fd in sorted(exclude):
        if (exclude_fd > remaining_range.stop):
            # This and all later exclusions are higher than the remaining
            # range.
            break
        if (exclude_fd < remaining_range.start):
            # The remaining range does not include the current exclusion.
            continue
        if (exclude_fd != remaining_range.start):
            # There is a candidate range below the current exclusion.
            ranges.append((remaining_range.start, exclude_fd))
        # Narrow the remaining range to those above the current exclusion.
        remaining_range = range(exclude_fd + 1, remaining_range.stop)

    if (remaining_range.start < remaining_range.stop):
        # After excluding all the specified file descriptors, there is a
        # remaining range; append it as a candidate for closing file
        # descriptors.
        ranges.append((remaining_range.start, remaining_range.stop))

    return ranges


def _close_file_descriptor_ranges(ranges):
    """ Close file descriptors described by `ranges`.

        :param ranges: A sequence of tuples `(low, high)`, each
            describing a range of file descriptors to close.
        :return: ``None``.

        Attempt to close each open file descriptor – starting from
        `low` and ending before `high` – from each range in `ranges`.
        """
    for range in ranges:
        os.closerange(range[0], range[1])


def close_all_open_files(exclude=None):
    """ Close all open file descriptors.

        :param exclude: Collection of file descriptors to skip when closing
            files.
        :return: ``None``.

        Closes every file descriptor (if open) of this process. If
        specified, `exclude` is a set of file descriptors to *not*
        close.
        """
    if exclude is None:
        exclude = set()
    fd_ranges = _get_candidate_file_descriptor_ranges(exclude=exclude)
    _close_file_descriptor_ranges(ranges=fd_ranges)


def redirect_stream(system_stream, target_stream):
    """ Redirect a system stream to a specified file.

        :param standard_stream: A file object representing a standard I/O
            stream.
        :param target_stream: The target file object for the redirected
            stream, or ``None`` to specify the null device.
        :return: ``None``.

        `system_stream` is a standard system stream such as
        ``sys.stdout``. `target_stream` is an open file object that
        should replace the corresponding system stream object.

        If `target_stream` is ``None``, defaults to opening the
        operating system's null device and using its file descriptor.
        """
    if target_stream is None:
        target_fd = os.open(os.devnull, os.O_RDWR)
    else:
        target_fd = target_stream.fileno()
    os.dup2(target_fd, system_stream.fileno())


def make_default_signal_map():
    """ Make the default signal map for this system.

        :return: A mapping from signal number to handler object.

        The signals available differ by system. The map will not contain
        any signals not defined on the running system.
        """
    name_map = {
            'SIGTSTP': None,
            'SIGTTIN': None,
            'SIGTTOU': None,
            'SIGTERM': 'terminate',
            }
    signal_map = dict(
            (getattr(signal, name), target)
            for (name, target) in name_map.items()
            if hasattr(signal, name))

    return signal_map


def set_signal_handlers(signal_handler_map):
    """ Set the signal handlers as specified.

        :param signal_handler_map: A map from signal number to handler
            object.
        :return: ``None``.

        See the `signal` module for details on signal numbers and signal
        handlers.
        """
    for (signal_number, handler) in signal_handler_map.items():
        signal.signal(signal_number, handler)


def register_atexit_function(func):
    """ Register a function for processing at program exit.

        :param func: A callable function expecting no arguments.
        :return: ``None``.

        The function `func` is registered for a call with no arguments
        at program exit.
        """
    atexit.register(func)


# Copyright © 2008–2023 Ben Finney <ben+python@benfinney.id.au>
# Copyright © 2007–2008 Robert Niederreiter, Jens Klein
# Copyright © 2004–2005 Chad J. Schroeder
# Copyright © 2003 Clark Evans
# Copyright © 2002 Noah Spurrier
# Copyright © 2001 Jürgen Hermann
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
