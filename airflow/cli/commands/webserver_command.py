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

"""Webserver command"""
import logging
import os
import signal
import subprocess
import sys
import textwrap
import time

import daemon
import psutil
from daemon.pidfile import TimeoutPIDLockFile
from lockfile.pidlockfile import read_pid_from_pidfile

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowWebServerTimeout
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations, setup_logging
from airflow.utils.process_utils import check_if_pidfile_process_is_running
from airflow.www.app import cached_app, create_app

log = logging.getLogger(__name__)


def get_num_ready_workers_running(gunicorn_master_proc):
    """Returns number of ready Gunicorn workers by looking for READY_PREFIX in process name"""
    workers = psutil.Process(gunicorn_master_proc.pid).children()

    def ready_prefix_on_cmdline(proc):
        try:
            cmdline = proc.cmdline()
            if len(cmdline) > 0:  # pylint: disable=len-as-condition
                return settings.GUNICORN_WORKER_READY_PREFIX in cmdline[0]
        except psutil.NoSuchProcess:
            pass
        return False

    ready_workers = [proc for proc in workers if ready_prefix_on_cmdline(proc)]
    return len(ready_workers)


def get_num_workers_running(gunicorn_master_proc):
    """Returns number of running Gunicorn workers processes"""
    workers = psutil.Process(gunicorn_master_proc.pid).children()
    return len(workers)


def restart_workers(gunicorn_master_proc, num_workers_expected, master_timeout):
    """
    Runs forever, monitoring the child processes of @gunicorn_master_proc and
    restarting workers occasionally.
    Each iteration of the loop traverses one edge of this state transition
    diagram, where each state (node) represents
    [ num_ready_workers_running / num_workers_running ]. We expect most time to
    be spent in [n / n]. `bs` is the setting webserver.worker_refresh_batch_size.
    The horizontal transition at ? happens after the new worker parses all the
    dags (so it could take a while!)
       V ────────────────────────────────────────────────────────────────────────┐
    [n / n] ──TTIN──> [ [n, n+bs) / n + bs ]  ────?───> [n + bs / n + bs] ──TTOU─┘
       ^                          ^───────────────┘
       │
       │      ┌────────────────v
       └──────┴────── [ [0, n) / n ] <─── start
    We change the number of workers by sending TTIN and TTOU to the gunicorn
    master process, which increases and decreases the number of child workers
    respectively. Gunicorn guarantees that on TTOU workers are terminated
    gracefully and that the oldest worker is terminated.
    """

    def wait_until_true(fn, timeout=0):
        """
        Sleeps until fn is true
        """
        start_time = time.time()
        while not fn():
            if 0 < timeout <= time.time() - start_time:
                raise AirflowWebServerTimeout(
                    "No response from gunicorn master within {0} seconds"
                    .format(timeout))
            time.sleep(0.1)

    def start_refresh(gunicorn_master_proc):
        batch_size = conf.getint('webserver', 'worker_refresh_batch_size')
        log.debug('%s doing a refresh of %s workers', state, batch_size)
        sys.stdout.flush()
        sys.stderr.flush()

        excess = 0
        for _ in range(batch_size):
            gunicorn_master_proc.send_signal(signal.SIGTTIN)
            excess += 1
            wait_until_true(lambda: num_workers_expected + excess ==
                            get_num_workers_running(gunicorn_master_proc),
                            master_timeout)

    try:  # pylint: disable=too-many-nested-blocks
        wait_until_true(lambda: num_workers_expected ==
                        get_num_workers_running(gunicorn_master_proc),
                        master_timeout)
        while True:
            num_workers_running = get_num_workers_running(gunicorn_master_proc)
            num_ready_workers_running = \
                get_num_ready_workers_running(gunicorn_master_proc)

            state = '[{0} / {1}]'.format(num_ready_workers_running, num_workers_running)

            # Whenever some workers are not ready, wait until all workers are ready
            if num_ready_workers_running < num_workers_running:
                log.debug('%s some workers are starting up, waiting...', state)
                sys.stdout.flush()
                time.sleep(1)

            # Kill a worker gracefully by asking gunicorn to reduce number of workers
            elif num_workers_running > num_workers_expected:
                excess = num_workers_running - num_workers_expected
                log.debug('%s killing %s workers', state, excess)

                for _ in range(excess):
                    gunicorn_master_proc.send_signal(signal.SIGTTOU)
                    excess -= 1
                    wait_until_true(lambda: num_workers_expected + excess ==
                                    get_num_workers_running(gunicorn_master_proc),
                                    master_timeout)

            # Start a new worker by asking gunicorn to increase number of workers
            elif num_workers_running == num_workers_expected:
                refresh_interval = conf.getint('webserver', 'worker_refresh_interval')
                log.debug(
                    '%s sleeping for %ss starting doing a refresh...',
                    state, refresh_interval
                )
                time.sleep(refresh_interval)
                start_refresh(gunicorn_master_proc)

            else:
                # num_ready_workers_running == num_workers_running < num_workers_expected
                log.error((
                    "%s some workers seem to have died and gunicorn"
                    "did not restart them as expected"
                ), state)
                time.sleep(10)
                if len(
                    psutil.Process(gunicorn_master_proc.pid).children()
                ) < num_workers_expected:
                    start_refresh(gunicorn_master_proc)
    except (AirflowWebServerTimeout, OSError) as err:
        log.error(err)
        log.error("Shutting down webserver")
        try:
            gunicorn_master_proc.terminate()
            gunicorn_master_proc.wait()
        finally:
            sys.exit(1)


@cli_utils.action_logging
def webserver(args):
    """Starts Airflow Webserver"""
    print(settings.HEADER)

    access_logfile = args.access_logfile or conf.get('webserver', 'access_logfile')
    error_logfile = args.error_logfile or conf.get('webserver', 'error_logfile')
    num_workers = args.workers or conf.get('webserver', 'workers')
    worker_timeout = (args.worker_timeout or
                      conf.get('webserver', 'web_server_worker_timeout'))
    ssl_cert = args.ssl_cert or conf.get('webserver', 'web_server_ssl_cert')
    ssl_key = args.ssl_key or conf.get('webserver', 'web_server_ssl_key')
    if not ssl_cert and ssl_key:
        raise AirflowException(
            'An SSL certificate must also be provided for use with ' + ssl_key)
    if ssl_cert and not ssl_key:
        raise AirflowException(
            'An SSL key must also be provided for use with ' + ssl_cert)

    if args.debug:
        print(
            "Starting the web server on port {0} and host {1}.".format(
                args.port, args.hostname))
        app = create_app(testing=conf.getboolean('core', 'unit_test_mode'))
        app.run(debug=True, use_reloader=not app.config['TESTING'],
                port=args.port, host=args.hostname,
                ssl_context=(ssl_cert, ssl_key) if ssl_cert and ssl_key else None)
    else:
        # This pre-warms the cache, and makes possible errors
        # get reported earlier (i.e. before demonization)
        os.environ['SKIP_DAGS_PARSING'] = 'True'
        app = cached_app(None)
        os.environ.pop('SKIP_DAGS_PARSING')

        pid_file, stdout, stderr, log_file = setup_locations(
            "webserver", args.pid, args.stdout, args.stderr, args.log_file)

        # Check if webserver is already running if not, remove old pidfile
        check_if_pidfile_process_is_running(pid_file=pid_file, process_name="webserver")

        print(
            textwrap.dedent('''\
                Running the Gunicorn Server with:
                Workers: {num_workers} {workerclass}
                Host: {hostname}:{port}
                Timeout: {worker_timeout}
                Logfiles: {access_logfile} {error_logfile}
                =================================================================\
            '''.format(num_workers=num_workers, workerclass=args.workerclass,
                       hostname=args.hostname, port=args.port,
                       worker_timeout=worker_timeout, access_logfile=access_logfile,
                       error_logfile=error_logfile)))

        run_args = [
            'gunicorn',
            '-w', str(num_workers),
            '-k', str(args.workerclass),
            '-t', str(worker_timeout),
            '-b', args.hostname + ':' + str(args.port),
            '-n', 'airflow-webserver',
            '-p', pid_file,
            '-c', 'python:airflow.www.gunicorn_config',
        ]

        if args.access_logfile:
            run_args += ['--access-logfile', str(args.access_logfile)]

        if args.error_logfile:
            run_args += ['--error-logfile', str(args.error_logfile)]

        if args.daemon:
            run_args += ['-D']

        if ssl_cert:
            run_args += ['--certfile', ssl_cert, '--keyfile', ssl_key]

        run_args += ["airflow.www.app:cached_app()"]

        gunicorn_master_proc = None

        def kill_proc(dummy_signum, dummy_frame):  # pylint: disable=unused-argument
            gunicorn_master_proc.terminate()
            gunicorn_master_proc.wait()
            sys.exit(0)

        def monitor_gunicorn(gunicorn_master_proc):
            # Register signal handlers
            signal.signal(signal.SIGINT, kill_proc)
            signal.signal(signal.SIGTERM, kill_proc)

            # These run forever until SIG{INT, TERM, KILL, ...} signal is sent
            if conf.getint('webserver', 'worker_refresh_interval') > 0:
                master_timeout = conf.getint('webserver', 'web_server_master_timeout')
                restart_workers(gunicorn_master_proc, num_workers, master_timeout)
            else:
                while gunicorn_master_proc.poll() is None:
                    time.sleep(1)

                sys.exit(gunicorn_master_proc.returncode)

        if args.daemon:
            handle = setup_logging(log_file)

            base, ext = os.path.splitext(pid_file)
            with open(stdout, 'w+') as stdout, open(stderr, 'w+') as stderr:
                ctx = daemon.DaemonContext(
                    pidfile=TimeoutPIDLockFile(f"{base}-monitor{ext}", -1),
                    files_preserve=[handle],
                    stdout=stdout,
                    stderr=stderr,
                )
                with ctx:
                    subprocess.Popen(run_args, close_fds=True)

                    # Reading pid of gunicorn master as it will be different that
                    # the one of process spawned above.
                    while True:
                        time.sleep(0.1)
                        gunicorn_master_proc_pid = read_pid_from_pidfile(pid_file)
                        if gunicorn_master_proc_pid:
                            break

                    # Run Gunicorn monitor
                    gunicorn_master_proc = psutil.Process(gunicorn_master_proc_pid)
                    monitor_gunicorn(gunicorn_master_proc)

        else:
            gunicorn_master_proc = subprocess.Popen(run_args, close_fds=True)
            monitor_gunicorn(gunicorn_master_proc)
