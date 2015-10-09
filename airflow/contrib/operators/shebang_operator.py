from airflow.utils import AirflowException
from airflow.models import BaseOperator
from airflow.utils import apply_defaults, TemporaryDirectory

from builtins import bytes
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile
from os import path

import logging


class ShebangOperator(BaseOperator):
    """
    Execute a script, command or set of commands with any given interpreter.

    :param command: The command, set of commands or reference to a
        some script (must be '.shebang' or '.sh') to be executed.
    :type command: string
    :param interpreter: interpreter name or an array of interpreter and options
        that executes command. Uses bash by default.
    :type interpreter: string
    :param suffix: suffix that is added to script file
    :type suffix: string
    :param prefix: prefix that is used as a name of script file
    :type prefix: string
    :param tmp_name: If true, than temporary name is generated for script file,
        otherwise output filename equals prefix plus suffix.
    :type tmp_name: boolean
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior.
    :type env: dict
    """
    template_fields = ('command',)
    template_ext = ('.shebang', '.sh', '.bash', '.rb', '.tex', '.R', )
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            command,
            interpreter=None,
            suffix=None,
            prefix=None,
            tmp_name=True,
            tmp_dir=None,
            xcom_push=False,
            env=None,
            *args, **kwargs):
        """
        If xcom_push is True, the last line written to stdout will also
        be pushed to an XCom when the command completes.
        """
        super(ShebangOperator, self).__init__(*args, **kwargs)
        self.command = command
        self.interpreter = interpreter or ["bash"]
        self.env = env
        self.xcom_push = xcom_push
        self.prefix = prefix or self.task_id
        self.suffix = suffix or ''
        self.tmp_name = tmp_name
        self.tmp_dir = tmp_dir
        if isinstance(self.interpreter, basestring):
            self.interpreter = [self.interpreter]

    def execute(self, context):
        """
        Execute the command in a temporary directory
        which will be cleaned afterwards
        """
        command = self.command
        logging.info("tmp dir root location: \n" + gettempdir())
        with TemporaryDirectory(prefix='airflowtmp',
                                dir=self.tmp_dir) as tmp_dir:
            if self.tmp_name:
                f = NamedTemporaryFile(dir=tmp_dir, prefix=self.prefix,
                                       suffix=self.suffix, delete=False)
                fname = f.name
            else:
                fname = path.join(tmp_dir, self.prefix + self.suffix)
                f = open(fname, 'w+b')

            f.write(bytes(command, 'utf_8'))
            f.flush()

            logging.info("Temporary script "
                         "location :{0}".format(fname))
            logging.info("Using interpreter: {}".format(
                ' '.join(map(str, self.interpreter))))
            logging.info("Running command: " + command)

            self.interpreter.append(fname)
            sp = Popen(
                self.interpreter,
                stdout=PIPE, stderr=STDOUT,
                cwd=tmp_dir, env=self.env)

            self.sp = sp

            logging.info("Output:")
            line = ''
            for line in iter(sp.stdout.readline, b''):
                logging.info(line.strip())
            sp.wait()
            logging.info("Command exited with "
                         "return code {0}".format(sp.returncode))

            if sp.returncode:
                raise AirflowException("Shebang command failed")

        if self.xcom_push:
            return str(line.strip())

    def on_kill(self):
        logging.info('Sending SIGTERM signal to shebang subprocess')
        self.sp.terminate()
