# -*- coding: utf-8 -*-
import os
import logging

import boto3
import watchtower
from jinja2 import Template
from airflow import configuration
from airflow.utils.log.logging_mixin import LoggingMixin


class CloudwatchTaskHandler(logging.Handler, LoggingMixin):
    def __init__(self, log_group, filename_template, region_name=None, **kwargs):
        super(CloudwatchTaskHandler, self).__init__()
        self.handler = None
        self.log_group = log_group
        self.region_name = region_name
        self.filename_template = filename_template
        self.filename_jinja_template = None
        self.kwargs = kwargs
        self.closed = False

        if "{{" in self.filename_template: #jinja mode
            self.filename_jinja_template = Template(self.filename_template)

    def _render_filename(self, ti, try_number):
        if self.filename_jinja_template:
            jinja_context = ti.get_template_context()
            jinja_context['try_number'] = try_number
            return (
                self.filename_jinja_template.render(**jinja_context)
                .replace(':', '_')
            )

        return self.filename_template.format(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            execution_date=ti.execution_date.isoformat(),
            try_number=try_number,
        ).replace(':', '_')

    def set_context(self, ti):
        kwargs = self.kwargs.copy()
        stream_name = kwargs.pop('stream_name', None)
        if stream_name is None:
            stream_name = self._render_filename(ti, ti.try_number)
        if 'boto3_session' not in kwargs and self.region_name is not None:
            kwargs['boto3_session'] = boto3.session.Session(
                region_name=self.region_name,
            )
        self.handler = watchtower.CloudWatchLogHandler(
            log_group=self.log_group,
            stream_name=stream_name,
            **kwargs
        )
    
    def emit(self, record):
        if self.handler is not None:
            self.handler.emit(record)

    def flush(self):
        if self.handler is not None:
            self.handler.flush()

    def close(self):
        """
        Close and upload local log file to remote storage S3.
        """
        # When application exit, system shuts down all handlers by
        # calling close method. Here we check if logger is already
        # closed to prevent uploading the log to remote storage multiple
        # times when `logging.shutdown` is called.
        if self.closed:
            return

        if self.handler is not None:
            self.handler.close()
        # Mark closed so we don't double write if close is called twice
        self.closed = True

    def read(self, task_instance, try_number=None):
        if try_number is None:
            next_try = task_instance.next_try_number
            try_numbers = list(range(1, next_try))
        elif try_number < 1:
            logs = [
                'Error fetching the logs. Try number {try_number} is invalid.',
            ]
            return logs
        else:
            try_numbers = [try_number]

        logs = [''] * len(try_numbers)
        for i, try_number in enumerate(try_numbers):
            logs[i] += self._read(task_instance, try_number)
        
        return logs
    
    def _read(self, task_instance, try_number):
        stream_name = self._render_filename(task_instance, try_number)
        if self.handler is not None:
            client = self.handler.cwl_client
        else:
            client = boto3.client('logs', region_name=self.region_name)
        events = []
        try:
            response = client.get_log_events(
                logGroupName=self.log_group,
                logStreamName=stream_name,
            )
            events.extend(response['events'])
            next_token = response['nextForwardToken']
            while True:
                response = client.get_log_events(
                    logGroupName=self.log_group,
                    logStreamName=stream_name,
                )
                if next_token == response['nextForwardToken']:
                    break
                events.extend(response['events'])
                next_token = response['nextForwardToken']
        except client.exceptions.ResourceNotFoundException:
            return f'Log stream {stream_name} does not exist'
        return '\n'.join(event['message'] for event in events)
