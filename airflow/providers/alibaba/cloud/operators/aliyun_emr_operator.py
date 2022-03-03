#
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
from time import gmtime, sleep, strftime


from airflow.models import BaseOperator
from airflow.providers.alibaba.cloud.hooks.aliyun_emr_hook import AliyunEmrHook
from airflow.utils.decorators import apply_defaults
from airflow.utils.trigger_rule import TriggerRule


class CreateClusterFromTemplateOperator(BaseOperator):

    # template_fields = ('to', 'subject', 'html_content')
    # template_ext = ('.html',)
    # ui_color = '#e6faf9'

    @apply_defaults
    def __init__(self, template_id, xcom_cluster_id_key='xcom_cluster_id', *args, **kwargs):
        super().__init__(*args, **kwargs)
        timestamp = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
        self.cluster_name = f'cluster-created-by-airflow_{timestamp}'
        self.template_id = template_id
        self.xcom_cluster_id_key = xcom_cluster_id_key
        self.aliyun_emr_hook = AliyunEmrHook()
        self.cluster_id = None
        self.cluster_type = None

    def execute(self, context):
        # create cluster from template
        self.cluster_id = self.aliyun_emr_hook.create_cluster_from_template(
            self.template_id, self.cluster_name
        )
        # put cluster_id into xcom channel
        ti = context['ti']
        ti.xcom_push(key=self.xcom_cluster_id_key, value=self.cluster_id)
        self.log.info(f"xcom_task_key: {self.xcom_cluster_id_key}")
        # check cluster status
        self.log.info('Checking cluster status...')
        retry = True
        while True:
            try:
                response = self.aliyun_emr_hook.get_cluster_info(self.cluster_id)
                status = response['ClusterInfo']['Status']
                self.log.info(f'Cluster status: {status}')
                if status == 'IDLE':
                    self.log.info('Cluster setup done!')
                    self.cluster_type = response['ClusterInfo']['SoftwareInfo']['ClusterType']
                    break
            except AttributeError as error:
                self.log.error(error)
                break
            except Exception as exception:
                self.log.error(exception)
                if retry == True:
                    retry == False
                    continue
                break
            sleep(10)
        sleep(30)
        self.log.info(f"cluster {self.cluster_id} is created")

    def on_kill(self):
        self.log.info("Cluster creating process get stopped externally, releasing cluster...")
        self.aliyun_emr_hook.tear_down_cluster(self.cluster_id)


class TearDownClusterOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        create_cluster_task_id,
        xcom_cluster_id_key='xcom_cluster_id',
        trigger_rule=TriggerRule.ALL_DONE,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.xcom_cluster_id_key = xcom_cluster_id_key
        self.create_cluster_task_id = create_cluster_task_id
        self.trigger_rule = trigger_rule
        self.aliyun_emr_hook = AliyunEmrHook()

    def execute(self, context):
        self.log.info('tearing down cluster...')
        ti = context['ti']
        self.log.info(f"create_cluster_task_id: {self.create_cluster_task_id}")
        self.log.info(f"xcom_task_key: {self.xcom_cluster_id_key}")
        cluster_id = ti.xcom_pull(task_ids=self.create_cluster_task_id, key=self.xcom_cluster_id_key)
        self.log.info('ready to tear down cluster with id: ')
        self.log.info(cluster_id)
        self.aliyun_emr_hook.tear_down_cluster(cluster_id)
        self.log.info("Cluster successfully torn down...")
