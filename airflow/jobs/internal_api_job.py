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
import os
import signal
import sys
from concurrent import futures

import grpc

from airflow.dag_processing.processor import DagFileProcessor
from airflow.internal_api.grpc.internal_api_pb2_grpc import add_FileProcessorServiceServicer_to_server
from airflow.internal_api.server import FileProcessorServiceServicer
from airflow.jobs.base_job import BaseJob


class InternalAPIJob(BaseJob):
    """InternalAPIJob exposes GRPC API to run Database operations."""

    __mapper_args__ = {'polymorphic_identity': 'InternalApiJob'}

    def __init__(self, *args, **kwargs):
        # Call superclass
        super().__init__(*args, **kwargs)

        # Set up runner async thread
        self.server = None

    def serve(self):
        processor = DagFileProcessor(dag_ids=[], log=self.log)
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_FileProcessorServiceServicer_to_server(FileProcessorServiceServicer(processor), self.server)
        self.server.add_insecure_port('[::]:50051')
        self.server.start()
        self.server.wait_for_termination()

    def register_signals(self) -> None:
        """Register signals that stop child processes"""
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def on_kill(self):
        """
        Called when there is an external kill command (via the heartbeat
        mechanism, for example)
        """
        self.server.stop()

    def _exit_gracefully(self, signum, frame) -> None:
        """Helper method to clean up processor_agent to avoid leaving orphan processes."""
        # The first time, try to exit nicely
        if not self.runner.stop:
            self.log.info("Exiting gracefully upon receiving signal %s", signum)
            self.server.stop()
        else:
            self.log.warning("Forcing exit due to second exit signal %s", signum)
            sys.exit(os.EX_SOFTWARE)

    def _execute(self) -> None:
        self.log.info("Starting the API")
        try:
            # Serve GRPC Server
            self.serve()
        except KeyboardInterrupt:
            self.log.info("GRPC server terminated")
        except Exception:
            self.log.exception("Exception when executing InternalAPIJob.execute")
            raise
        finally:
            # Tell the subthread to stop and then wait for it.
            # If the user interrupts/terms again, _graceful_exit will allow them
            # to force-kill here.
            self.log.info("Exited GRPC loop")


if __name__ == '__main__':
    job = InternalAPIJob()
    job.run()
