import requests
import time
import logging
import datetime
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator


class ScrapydPerformCrawlOperator(BaseOperator):
    """
    Operator that schedules a scrapy spider run on scrapyd. Once scheduled, the operators polls the status of the
    crawl job and waits until it finishes if the await_completion flag is set to True.
    """

    def __init__(self,
                 scrapyd_url,
                 project_name,
                 spider_name,
                 await_completion=True,
                 *args,
                 **kwargs):
        """
        Create a ScrapydPerformCrawlOperator that schedules a spider run on a given scrapyd server and optionally awaits
        for its completion.

        :param scrapyd_url: The url (including the port if needed) of the scrapyd server
        :type scrapyd_url: string
        :param project_name: The scrapyd project name under which the spider is deployed.
        :type project_name: string
        :param spider_name: The name of the spider to run
        :type spider_name: string
        :param await_completion: Boolean flag that determines whether the operator should wait untill the spider
                finishes
        :type await_completion: bool
        """
        super(ScrapydPerformCrawlOperator, self).__init__(*args, **kwargs)
        self.scrapyd_url = scrapyd_url
        self.project_name = project_name
        self.spider_name = spider_name
        self.await_completion=await_completion

    def execute(self, context):
        post_data = {
            'project': self.project_name,
            'spider': self.spider_name
        }
        schedule_url = self.scrapyd_url + '/schedule.json'
        logging.info("Scheduling crawl for spider '{:s}' in project '{:s}'".format(
            post_data['spider'],
            post_data['project']
        ))
        response = requests.post(schedule_url, data=post_data)
        result = response.json()
        if result['status'] == 'ok':
            job_id = result['jobid']
            logging.info("Successfully scheduled job with id '{:s}'".format(job_id))
            if self.await_completion:
                crawl_start_time = datetime.datetime.now()
                while True:
                    try:
                        jobs = requests.get(
                            self.scrapyd_url + '/listjobs.json',
                            params={'project': 'competitor_scraping'}
                        ).json()
                        assert(jobs['status'] == 'ok')
                    except Exception as e:
                        raise AirflowException('Error while performing GET request to scrapyd: ' + e.message)
                    job_states = dict({job['id']: 'finished' for job in jobs['finished']}.items() +
                                      {job['id']: 'pending' for job in jobs['pending']}.items() +
                                      {job['id']: 'running' for job in jobs['running']}.items())
                    if job_id in job_states:
                        job_state = job_states[job_id]
                        current_time = datetime.datetime.now()
                        time_passed = current_time - crawl_start_time
                        logging.info("Job '{:s}' has status: {:s} (crawl time: {:s})".format(
                            job_id,
                            job_state,
                            str(time_passed)
                        ))
                        if job_state in ('pending', 'running'):
                            time.sleep(15)
                            continue
                        elif job_state == 'finished':
                            break
                        else:
                            #  unknown job state...
                            raise AirflowException('Unknown scrapyd job state: {:s}'.format(job_state))
                    else:
                        # invalid job_id...
                        raise AirflowException('Unknown scrapyd job id: {:s}'.format(job_id))
        else:
            if 'message' in result:
                raise AirflowException("Error while scheduling job on scrapyd: {:s}".format(result['message']))
            else:
                raise AirflowException("Unknown error while scheduling job on scrapyd")
