import requests
import time
import logging
from airflow.providers.plexus.hooks.plexus import PlexusHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.AirflowExceptions import AirflowAirflowException

logger = logging.getLogger(__name__)

class PlexusJobOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            job_params,
            **kwargs):
        super().__init__(**kwargs)

        self.job_params = job_params
        self.required_params = set(["name", "app", "queue", "num_cores", "num_nodes"])
        self.lookups = {"app": ("apps/", "id", "name"),
                        "billing_account_id": ("users/{}/billingaccounts/", "id", None),  
                        "queue": ("queues/", "id", "public_name")}
        self.job_params.update({"billing_account_id": None})

    def execute(self, context):
        hook = PlexusHook()
        params = self.construct_job_params(hook)
        jobs_endpoint = hook.host + "jobs/"
        headers = {"Authorization": "Bearer {}".format(hook.token)}

        logger.info("creating job w/ following params: {}".format(params))
        create_job = requests.post(jobs_endpoint, headers=headers, data=params, timeout=10)
        if create_job.ok:
            job = create_job.json()
            jid = job["id"]
            state = job["last_state"]
            while state != "Completed":
                time.sleep(5)
                jid_endpoint = jobs_endpoint + "{}/".format(jid)
                get_job = requests.get(jid_endpoint, headers=headers)
                if not get_job.ok:
                    raise AirflowException("Could not retrieve job status. Status Code: [{}]. Reason: {} - {}".format(get_job.status_code, get_job.reason, get_job.text))
                else:
                    new_state = get_job.json()["last_state"]
                    if new_state == "Cancelled":
                        raise AirflowException("Job has been cancelled")
                    elif new_state == "Failed":
                        raise AirflowException("Job Failed")
                    elif new_state != state:
                        logger.info("job is {}".format(new_state))
                    state = new_state
        else:
            raise AirflowException("Could not start job. Status Code: [{}]. Reason: {} - {}".format(create_job.status_code, create_job.reason, create_job.text))


    def _api_lookup(self, endpoint, token, key, mapping=None):
        headers = {"Authorization": "Bearer {}".format(token)}
        r = requests.get(endpoint, headers=headers, timeout=5)
        results = r.json()["results"]

        v = None
        if mapping is None:
            v = results[0][key]
        else: 
            for d in results:
                if d[mapping[0]] == mapping[1]:
                    v = d[key]
        if v is None:
            raise AirflowException("Could not locate value for param:{} at endpoint: {}".format(key, endpoint))
        
        return v


    def construct_job_params(self, hook):
        missing_params = self.required_params - set(self.job_params)
        if len(missing_params) > 0:
            raise AirflowException("Missing the following required job_params: {}".format(", ".join(missing_params)))
        params = {}
        for pm in self.job_params:
            if pm in self.lookups:
                lookup = self.lookups[pm]
                if pm == "billing_account_id":
                    endpoint = hook.host + lookup[0].format(hook.user_id)
                else:
                    endpoint = hook.host + lookup[0]
                mapping = None if lookup[2] is None else (lookup[2], self.job_params[pm])
                v = self._api_lookup(endpoint=endpoint, token=hook.token, key=lookup[1], mapping=mapping)
                params[pm] = v 
            else:
                params[pm] = self.job_params[pm]
        return params


            





