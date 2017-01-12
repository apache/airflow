from airflow.models import BaseOperator
from airflow.utils import apply_defaults
try:
    from airflow.utils import AirflowException
except Exception, e:
    # location of AirflowException changed in v1.7.0
    from airflow.exceptions import AirflowException
import json
import logging
import requests

class StatusPageComponentOperator(BaseOperator):
    """
    This operator is the allows updating a StatusPage Component via the api.
    docs for it are here https://doers.statuspage.io/api/v1/components/
    """
    template_fields = ['component_status']

    @apply_defaults
    def __init__(self,
                 apikey,
                 pageid,
                 endpoint='https://api.statuspage.io/v1/pages/',
                 component_id='',
                 component_status='',
                 *args, **kwargs):
        super(StatusPageComponentOperator, self).__init__(*args, **kwargs)
        self.apikey = apikey
        self.pageid = pageid
        self.endpoint= endpoint
        self.component_id = component_id
        self.component_status = component_status

    def execute(self, context):

        url = '{endpoint}{pageid}/components/{component_id}.json'.format(endpoint=self.endpoint, pageid=self.pageid,component_id=self.component_id)

        header = {'Content-Type': 'application/json', 'Authorization': 'OAuth {apikey}'.format(apikey=self.apikey)}
        body = json.dumps({
          "component" : {
            "status" : self.component_status
          }
        })

        response = requests.request("PATCH",url,data=body,headers=header)
        if response.status_code >= 400:
            logging.error('StatusPage API call failed: %s %s',
                          response.status_code, response.reason)
            raise AirflowException('StatusPage API call failed: %s %s' %
                                   (response.status_code, response.reason))

