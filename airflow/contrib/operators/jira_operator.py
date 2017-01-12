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
import base64

class JIRATransitionOperator(BaseOperator):
    """
    This operator is the allows updating a StatusPage Component via the api.
    docs for it are here https://doers.statuspage.io/api/v1/components/
    """
    template_fields = ['transition_id']

    @apply_defaults
    def __init__(self,
                 api_user,
                 api_password,
                 endpoint='',
                 ticket_id='',
                 transition_id='',
                 *args, **kwargs):
        super(JIRATransitionOperator, self).__init__(*args, **kwargs)
        self.api_user = api_user
        self.api_password = api_password
        self.endpoint = endpoint
        self.ticket_id = ticket_id
        self.transition_id = transition_id

    def execute(self, context):

        url = '{endpoint}/rest/api/latest/issue/{ticket_id}/transitions?expand=transitions.fields'.format(endpoint=self.endpoint, ticket_id=self.ticket_id)

        header = {'Content-Type': 'application/json', 'Authorization': 'Basic {creds}'.format(creds = base64.b64encode(self.api_user + ":" + self.api_password))}
        body = json.dumps({
          "transition" : {
            "id" : self.transition_id
          }
        })

        response = requests.request("POST",url,data=body,headers=header)
        if response.status_code >= 400:
            logging.error('JIRA API call failed: %s %s',
                          response.status_code, response.reason)
            raise AirflowException('JIRA API call failed: %s %s' %
                                   (response.status_code, response.reason))

