#
# Licensed to the Apache Software Foundation (ASF) under one or more
# TODO: This license is not consistent with license used in the project.
#       Delete the inconsistent license and above line and rerun pre-commit to insert a good license.
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import re


class Note:
    """
    Json format note result which include list of paragraph result, this is returned by Zeppelin rest api
    """

    def __init__(self, note_json):
        self.note_json = note_json
        self.id = note_json['id']
        self.name = note_json['name']
        self.is_running = False
        if 'info' in note_json:
            info_json = note_json['info']
            self.is_running = bool(info_json.get('isRunning', 'False'))
        self.paragraphs = []
        if 'paragraphs' in note_json:
            paragraph_json_array = note_json['paragraphs']
            self.paragraphs = list(map(lambda x: Paragraph(x), paragraph_json_array))
        self.ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

    def is_success(self):
        for p in self.paragraphs:
            if p.status != 'FINISHED':
                return False
        return True

    def get_errors(self):
        for p in self.paragraphs:
            if p.status != 'FINISHED':
                return "Paragraph {} is {}.\nText: {}\nResults:{}\nAssociated job urls: {}".format(
                    p.id, p.status, p.text, '\n'.join(list(map(lambda x: x[1], p.results))), str(p.job_urls)
                )
        return "All paragraphs are finished successfully!"

    def get_content(self):
        content = []
        for p in self.paragraphs:
            content.append(
                "Paragraph {} is {}.\nText:\n{}\nResults:\n{}\nAssociated job urls:\n{}".format(
                    p.id,
                    p.status,
                    p.text,
                    '\n'.join(list(map(lambda x: self.ansi_escape.sub('', x[1]), p.results))),
                    str(p.job_urls),
                )
            )
        return ('\n' + '-' * 120 + '\n').join(content)

    def __repr__(self):
        return json.dumps(self.note_json, indent=2)


class Paragraph:
    """
    Json format of paragraph result which returned by Zeppelin rest api.
    """

    def __init__(self, paragraph_json):
        self._paragraph_json = paragraph_json
        self.id = paragraph_json['id']
        self.text = paragraph_json.get('text')
        self.status = paragraph_json.get('status')
        self.progress = 0
        if 'progress' in paragraph_json:
            self.progress = int(paragraph_json['progress'])
        if 'results' in paragraph_json:
            results_json = paragraph_json['results']
            msg_array = results_json['msg']
            self.results = list(map(lambda x: (x['type'], x['data']), msg_array))
        else:
            self.results = []

        self.job_urls = []
        if 'runtimeInfos' in paragraph_json:
            runtime_infos_json = paragraph_json['runtimeInfos']
            if 'jobUrl' in runtime_infos_json:
                job_url_json = runtime_infos_json['jobUrl']
                if 'values' in job_url_json:
                    job_url_values = job_url_json['values']
                    self.job_urls = list(
                        map(lambda x: x['jobUrl'], filter(lambda x: 'jobUrl' in x, job_url_values))
                    )

    def is_completed(self):
        return self.status in ['FINISHED', 'ERROR', 'ABORTED']

    def is_running(self):
        return self.status == 'RUNNING'

    def is_success(self):
        return self.status == 'FINISHED'

    def get_errors(self):
        if self.status != 'FINISHED':
            return "Paragraph {} is failed.\n\nText: {}\n\nResults:{}\n\nAssociated job urls: {}\n\nJson:{}".\
                format(
                self.id,
                self.text,
                '\n'.join(list(map(lambda x: x[1], self.results))),
                str(self._job_urls),
                self._paragraph_json,
            )
        return "Paragraph is finished successfully!"

    def get_status(self):
        return self.status

    def __repr__(self):
        return json.dumps(self._paragraph_json, indent=2)


class ExecuteResult:
    """
    Paragraph result.
    """

    def __init__(self, paragraph_result):
        self.statement_id = paragraph_result.id
        self.status = paragraph_result.status
        self.progress = paragraph_result.progress
        self.results = paragraph_result.results
        self.job_urls = paragraph_result.jobUrls

    def __repr__(self):
        return str(self.__dict__)

    def is_success(self):
        return self.status == 'FINISHED'
