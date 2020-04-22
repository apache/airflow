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

import contextlib
import cProfile
import datetime
import io
import os
import pstats
import signal

PYSPY_OUTPUT = os.environ.get("PYSPY_OUTPUT", "/files/pyspy/")


@contextlib.contextmanager
def pyspy():
    """
    This decorator provide deterministic profiling. It generate and save flame graph to file. It uses``pyspy``
    internally.

    Running py-spy inside of a docker container will also usually bring up a permissions denied error
    even when running as root.

    This error is caused by docker restricting the process_vm_readv system call we are using. This can be
    overridden by setting --cap-add SYS_PTRACE when starting the docker container.

    Alternatively you can edit the docker-compose yaml file

    .. code-block:: yaml

        your_service:
          cap_add:
          - SYS_PTRACE

    In the case of Airflow Breeze, you should modify the ``scripts/perf/perf_kit/python.py`` file.
    """
    pid = str(os.getpid())
    suffix = datetime.datetime.now().isoformat()
    filename = f"{PYSPY_OUTPUT}/flame-{suffix}-{pid}.html"
    pyspy_pid = os.spawnlp(
        os.P_NOWAIT, "sudo", "sudo", "py-spy", "record", "--idle", "-o", filename, "-p", pid
    )
    try:
        yield
    finally:
        os.kill(pyspy_pid, signal.SIGINT)
        print(f"Report saved to: {filename}")


@contextlib.contextmanager
def profiled(print_callers=False):
    """
    This decorator provide deterministic profiling. It uses ``cProfile`` internally.  It generates statistic
    and print on the screen.
    """
    pr = cProfile.Profile()
    pr.enable()
    try:
        yield
    finally:
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
        if print_callers:
            ps.print_callers()
        else:
            ps.print_stats()
        print(s.getvalue())


if __name__ == "__main__":

    def case():
        import airflow
        from airflow.jobs.scheduler_job import DagFileProcessor
        import logging

        log = logging.getLogger(__name__)
        processor = DagFileProcessor(dag_ids=[], log=log)
        dag_file = os.path.join(os.path.dirname(airflow.__file__), "example_dags", "example_complex.py")
        processor.process_file(file_path=dag_file, failure_callback_requests=[])

    # Load modules
    case()

    # Example:
    print("PySpy:")
    with pyspy():
        case()

    # Example:
    print("cProfile")
    with profiled():
        case()
