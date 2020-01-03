import sys
import signal
import subprocess
import time
from airflow.utils import cli as cli_utils

@cli_utils.action_logging
def knative_worker(args):
    num_workers = args.workers or 8
    # worker_timeout = (args.worker_timeout or
    #                   conf.get('webserver', 'web_server_worker_timeout'))
    worker_timeout = 10000
    hostname = args.hostname or "0.0.0.0"
    port = args.port or "8081"
    run_args = [
        'gunicorn',
        '-w', str(num_workers),
        '-k', 'sync',
        '-t', str(worker_timeout),
        '-b', str(hostname) + ':' + str(port),
        '-n', 'airflow-worker',
        '-c', 'python:airflow.www.gunicorn_config',
        'airflow.knative_worker.knative_task_runner:create_app()'
    ]

    def monitor_gunicorn(gunicorn_master_proc):
        while gunicorn_master_proc.poll() is None:
            time.sleep(1)
        sys.exit(gunicorn_master_proc.returncode)

    def kill_proc(dummy_signum, dummy_frame):
        gunicorn_master_proc.terminate()
        gunicorn_master_proc.wait()
        sys.exit(0)

    gunicorn_master_proc = subprocess.Popen(run_args, close_fds=True)

    signal.signal(signal.SIGINT, kill_proc)
    signal.signal(signal.SIGTERM, kill_proc)

    monitor_gunicorn(gunicorn_master_proc)
