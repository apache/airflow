from builtins import bytes
import logging
import subprocess
from subprocess import STDOUT

from airflow.models import BaseOperator
from airflow.contrib.hooks import SSHHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class SSHTempFileContent(object):
    """This class prvides a functionality that creates tempfile
    with given content at remote host.
    Use like::

    with SSHTempFileContent(ssh_hook, content) as tempfile:
        ...

    In this case, a temporary file ``tempfile``
    with content ``content`` is created where ``ssh_hook`` designate.

    Note that this isn't safe because other processes
    at remote host can read and write that tempfile.

    :param ssh_hook: A SSHHook that indicates a remote host
                     where you want to create tempfile
    :param content: Initial content of creating temprary file
    :type content: string
    :param prefix: The prefix string you want to use for the temprary file
    :type prefix: string
    """

    def __init__(self, ssh_hook, content, prefix="tmp"):
        self._ssh_hook = ssh_hook
        self._content = content
        self._prefix = prefix

    def __enter__(self):
        ssh_hook = self._ssh_hook
        string = self._content
        prefix = self._prefix

        pmktemp = ssh_hook.Popen(["-q",
                                  "mktemp", "--tmpdir", prefix + "_XXXXXX"],
                                 stdout=subprocess.PIPE,
                                 stderr=STDOUT)
        tempfile = pmktemp.communicate()[0].rstrip()
        pmktemp.wait()
        if pmktemp.returncode:
            raise AirflowException("Failed to create remote temp file")

        ptee = ssh_hook.Popen(["-q", "tee", tempfile],
                              stdin=subprocess.PIPE,
                              # discard stdout
                              stderr=STDOUT)
        ptee.stdin.write(bytes(string, 'utf_8'))
        ptee.stdin.close()
        ptee.wait()
        if ptee.returncode:
            raise AirflowException("Failed to write to remote temp file")

        self._tempfile = tempfile
        return tempfile

    def __exit__(self, type, value, traceback):
        sp = self._ssh_hook.Popen(["-q", "rm", "-f", "--", self._tempfile])
        sp.communicate()
        sp.wait()
        if sp.returncode:
            raise AirflowException("Failed to remove to remote temp file")
        return False


class SSHExecuteOperator(BaseOperator):
    """
    Execute a Bash script, command or set of commands at remote host.

    :param ssh_hook: A SSHHook that indicates the remote host
                     you want to run the script
    :param ssh_hook: SSHHook
    :param ssh_conn_id: If ssh_hook is None, then destination ssh connection id
        will be used to instantiate a Hook
    :type ssh_conn_id: string
    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed.
    :type bash_command: string
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior.
    :type env: dict
    :param xcom_push_type: 'line', 'output'
    :type xcom_push_type: string
    """

    template_fields = ("bash_command", "env",)
    template_ext = (".sh", ".bash",)

    @apply_defaults
    def __init__(self,
                 bash_command,
                 ssh_hook=None,
                 ssh_conn_id=None,
                 xcom_push=False,
                 xcom_push_type='line',
                 env=None,
                 *args, **kwargs):
        super(SSHExecuteOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.env = env
        if ssh_hook is not None:
            self.hook = ssh_hook
        elif ssh_conn_id is not None:
            self.hook = SSHHook(conn_id=ssh_conn_id)
        else:
            raise AirflowException("Either ssh_hook or ssh_conn_id is required. If both are sent, ssh_conn_id will be ignored")

        self.xcom_push = xcom_push
        self.xcom_push_type = xcom_push_type

    def execute(self, context):
        bash_command = self.bash_command
        hook = self.hook
        host = hook._host_ref()

        with SSHTempFileContent(self.hook,
                                self.bash_command,
                                self.task_id) as remote_file_path:
            logging.info("Temporary script "
                         "location : {0}:{1}".format(host, remote_file_path))
            logging.info("Running command: " + bash_command)

            sp = hook.Popen(
                ['-q', 'bash', remote_file_path],
                stdout=subprocess.PIPE, stderr=STDOUT,
                env=self.env)

            self.sp = sp

            logging.info("Output:")
            line = ''
            output = []
            for line in iter(sp.stdout.readline, b''):
                line = line.decode().strip()
                output.append(line)
                logging.info(line)
            sp.wait()
            logging.info("Command exited with "
                         "return code {0}".format(sp.returncode))

            if sp.returncode:
                raise AirflowException("Bash command failed")

        if self.xcom_push:
            if self.xcom_push_type == 'line':
                return line
            else:
                return output

    def on_kill(self):
        # TODO: Cleanup remote tempfile
        # TODO: kill `mktemp` or `tee` too when they are alive.
        logging.info('Sending SIGTERM signal to bash subprocess')
        self.sp.terminate()
