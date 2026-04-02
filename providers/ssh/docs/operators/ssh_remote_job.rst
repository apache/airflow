 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.



.. _howto/operator:SSHRemoteJobOperator:

SSHRemoteJobOperator
====================

Use the :class:`~airflow.providers.ssh.operators.ssh_remote_job.SSHRemoteJobOperator` to execute
commands on a remote server via SSH as a detached job. This operator is **deferrable**, meaning it
offloads long-running job monitoring to the triggerer, freeing up worker slots for other tasks.

This operator is designed to be more resilient than :class:`~airflow.providers.ssh.operators.ssh.SSHOperator`
for long-running jobs, especially in environments where network interruptions or worker restarts may occur.

Key Features
------------

* **Deferrable**: Offloads job monitoring to the triggerer process
* **Detached Execution**: Starts remote jobs that continue running even if SSH connection drops
* **Incremental Log Streaming**: Tails logs from remote host and displays them in Airflow
* **Cross-Platform**: Supports both POSIX (Linux/macOS) and Windows remote hosts
* **Resilient**: Jobs survive network interruptions and worker restarts
* **File-based Completion**: Uses exit code file for reliable completion detection

When to Use This Operator
--------------------------

Use ``SSHRemoteJobOperator`` when:

* Running long-running jobs (minutes to hours) on remote hosts
* Network stability is a concern
* You need to see incremental logs as the job progresses
* The remote job should survive temporary disconnections
* You want to free up worker slots during job execution

Use the traditional :class:`~airflow.providers.ssh.operators.ssh.SSHOperator` when:

* Running short commands (seconds)
* You need bidirectional communication during execution
* The command requires an interactive TTY

How It Works
------------

1. **Job Submission**: The operator connects via SSH and submits a wrapper script that:

   * Creates a unique job directory on the remote host
   * Starts your command as a detached process (``nohup`` on POSIX, ``Start-Process`` on Windows)
   * Redirects output to a log file
   * Writes exit code to a file when complete

2. **Deferral**: The operator immediately defers to :class:`~airflow.providers.ssh.triggers.ssh_remote_job.SSHRemoteJobTrigger`

3. **Monitoring**: The trigger periodically:

   * Checks if the exit code file exists (job complete)
   * Reads new log content incrementally
   * Yields events with log chunks back to the operator

4. **Completion**: When the job finishes:

   * Final logs are displayed
   * Exit code is checked (0 = success, non-zero = failure)
   * Optional cleanup of remote job directory

Using the Operator
------------------

Basic Example
^^^^^^^^^^^^^

.. code-block:: python

    from airflow.providers.ssh.operators.ssh_remote_job import SSHRemoteJobOperator

    run_script = SSHRemoteJobOperator(
        task_id="run_remote_script",
        ssh_conn_id="my_ssh_connection",
        command="/path/to/script.sh",
        poll_interval=5,  # Check status every 5 seconds
        cleanup="on_success",  # Clean up remote files on success
    )

With Environment Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    run_with_env = SSHRemoteJobOperator(
        task_id="run_with_environment",
        ssh_conn_id="my_ssh_connection",
        command="python process_data.py",
        environment={
            "DATA_PATH": "/data/input.csv",
            "OUTPUT_PATH": "/data/output.csv",
            "LOG_LEVEL": "INFO",
        },
    )

Windows Remote Host
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    run_on_windows = SSHRemoteJobOperator(
        task_id="run_on_windows",
        ssh_conn_id="windows_ssh_connection",
        command="C:\\Scripts\\process.ps1",
        remote_os="windows",  # Explicitly specify Windows
        poll_interval=10,
    )

With Timeout and Skip on Exit Code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    run_with_options = SSHRemoteJobOperator(
        task_id="run_with_options",
        ssh_conn_id="my_ssh_connection",
        command="./long_running_job.sh",
        timeout=3600,  # Fail if not complete in 1 hour
        skip_on_exit_code=99,  # Skip task if job exits with code 99
        cleanup="always",  # Always clean up, even on failure
    )

Parameters
----------

* ``ssh_conn_id`` (str, required): The Airflow connection ID for SSH connection
* ``command`` (str, required): The command or script path to execute on the remote host
* ``remote_host`` (str, optional): Override the host from the connection
* ``environment`` (dict, optional): Environment variables to set for the command
* ``remote_base_dir`` (str, optional): Base directory for job files. Defaults to:

  * POSIX: ``/tmp/airflow-ssh-jobs``
  * Windows: ``$env:TEMP\\airflow-ssh-jobs``

* ``poll_interval`` (int, optional): How often to check job status in seconds. Default: 5
* ``log_chunk_size`` (int, optional): Maximum bytes to read from log per poll. Default: 65536
* ``timeout`` (int, optional): Hard timeout for the entire task in seconds. Default: None (no timeout)
* ``cleanup`` (str, optional): When to clean up remote job directory:

  * ``"never"`` (default): Never clean up
  * ``"on_success"``: Clean up only if job succeeds
  * ``"always"``: Always clean up regardless of status

* ``remote_os`` (str, optional): Remote OS type (``"auto"``, ``"posix"``, ``"windows"``). Default: ``"auto"``
* ``skip_on_exit_code`` (int or list, optional): Exit code(s) that should cause task to skip instead of fail

Remote OS Detection
-------------------

The operator can automatically detect the remote OS (``remote_os="auto"``), but explicit specification
is more reliable:

* Use ``remote_os="posix"`` for Linux and macOS hosts
* Use ``remote_os="windows"`` for Windows hosts with OpenSSH Server

For Windows, ensure:

* OpenSSH Server is installed and running
* PowerShell is available (default on modern Windows)
* SSH connection allows command execution

Job Directory Structure
-----------------------

Each job creates a directory on the remote host with these files:

.. code-block:: text

    /tmp/airflow-ssh-jobs/af_mydag_mytask_run123_try1_abc12345/
    ├── stdout.log         # Combined stdout/stderr
    ├── exit_code          # Final exit code (0 or non-zero)
    ├── pid                # Process ID (for on_kill)
    └── status             # Optional status file (for user scripts)

Your command can access these via environment variables:

* ``LOG_FILE``: Path to the log file
* ``STATUS_FILE``: Path to the status file

Connection Requirements
-----------------------

The SSH connection must support:

* Non-interactive authentication (password or key-based)
* Command execution without PTY
* File I/O on the remote host

See :ref:`howto/connection:ssh` for connection configuration.

Limitations and Considerations
-------------------------------

**Network Interruptions**: While the operator is resilient to disconnections during monitoring,
the initial job submission must succeed. If submission fails, the task will fail immediately.

**Remote Process Management**: Jobs are detached using ``nohup`` (POSIX) or ``Start-Process`` (Windows).
If the remote host reboots during job execution, the job will be lost.

**Log Size**: Large log outputs may impact performance. The ``log_chunk_size`` parameter controls
how much data is read per poll. For very large logs (GBs), consider having your script write
logs to a separate file and only log summaries to stdout.

**Exit Code Detection**: The operator uses file-based exit code detection for reliability.
If your script uses ``exec`` to replace the shell process, ensure the exit code is still
written to the file.

**Concurrent Jobs**: Each task instance creates a unique job directory. Multiple concurrent
tasks can run on the same remote host without conflicts.

**Cleanup**: Use ``cleanup="on_success"`` or ``cleanup="always"`` to avoid accumulating
job directories on the remote host. For debugging, use ``cleanup="never"`` and manually
inspect the job directory.

Comparison with SSHOperator
----------------------------

+---------------------------+------------------+---------------------+
| Feature                   | SSHOperator      | SSHRemoteJobOperator|
+===========================+==================+=====================+
| Execution Model           | Synchronous      | Asynchronous        |
+---------------------------+------------------+---------------------+
| Worker Slot Usage         | Entire duration  | Only during submit  |
+---------------------------+------------------+---------------------+
| Network Resilience        | Low              | High                |
+---------------------------+------------------+---------------------+
| Long-running Jobs         | Not recommended  | Designed for        |
+---------------------------+------------------+---------------------+
| Incremental Logs          | No               | Yes                 |
+---------------------------+------------------+---------------------+
| Windows Support           | Limited          | Full (via OpenSSH)  |
+---------------------------+------------------+---------------------+
| Setup Complexity          | Simple           | Moderate            |
+---------------------------+------------------+---------------------+

Related Documentation
---------------------

* :class:`~airflow.providers.ssh.operators.ssh.SSHOperator` - Traditional synchronous SSH operator
* :class:`~airflow.providers.ssh.triggers.ssh_remote_job.SSHRemoteJobTrigger` - Trigger used by this operator
* :class:`~airflow.providers.ssh.hooks.ssh.SSHHook` - SSH hook for synchronous operations
* :class:`~airflow.providers.ssh.hooks.ssh.SSHHookAsync` - Async SSH hook for triggers
* :ref:`howto/connection:ssh` - SSH connection configuration
