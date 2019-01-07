import os
import subprocess

from airflow import LoggingMixin, AirflowException


class LoggingCommandExecutor(LoggingMixin):

    def execute_cmd(self, cmd, silent=False):
        if silent:
            self.log.info("Executing in silent mode: '{}'".format(" ".join(cmd)))
            with open(os.devnull, 'w') as FNULL:
                return subprocess.call(args=cmd, stdout=FNULL, stderr=subprocess.STDOUT)
        else:
            self.log.info("Executing: '{}'".format(" ".join(cmd)))
            process = subprocess.Popen(args=cmd, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, universal_newlines=True)
            output, err = process.communicate()
            retcode = process.poll()
            self.log.info("Stdout: {}".format(output))
            self.log.info("Stderr: {}".format(err))
            if retcode:
                print("Error when executing '{}'".format(" ".join(cmd)))
            return retcode

    def check_output(self, cmd):
        self.log.info("Executing for output: '{}'".format(" ".join(cmd)))
        process = subprocess.Popen(args=cmd, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        output, err = process.communicate()
        retcode = process.poll()
        if retcode:
            self.log.info("Error when executing '{}'".format(" ".join(cmd)))
            self.log.info("Stdout: {}".format(output))
            self.log.info("Stderr: {}".format(err))
            raise AirflowException("Retcode {} on {} with stdout: {}, stderr: {}".
                                   format(retcode, " ".join(cmd), output, err))
        return output
