:mod:`airflow.providers.jenkins.operators.jenkins_job_trigger`
==============================================================

.. py:module:: airflow.providers.jenkins.operators.jenkins_job_trigger


Module Contents
---------------

.. data:: JenkinsRequest
   

   

.. data:: ParamType
   

   

.. function:: jenkins_request_with_headers(jenkins_server: Jenkins, req: Request) -> Optional[JenkinsRequest]
   We need to get the headers in addition to the body answer
   to get the location from them
   This function uses jenkins_request method from python-jenkins library
   with just the return call changed

   :param jenkins_server: The server to query
   :param req: The request to execute
   :return: Dict containing the response body (key body)
       and the headers coming along (headers)


.. py:class:: JenkinsJobTriggerOperator(*, jenkins_connection_id: str, job_name: str, parameters: ParamType = '', sleep_time: int = 10, max_try_before_job_appears: int = 10, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Trigger a Jenkins Job and monitor it's execution.
   This operator depend on python-jenkins library,
   version >= 0.4.15 to communicate with jenkins server.
   You'll also need to configure a Jenkins connection in the connections screen.

   :param jenkins_connection_id: The jenkins connection to use for this job
   :type jenkins_connection_id: str
   :param job_name: The name of the job to trigger
   :type job_name: str
   :param parameters: The parameters block provided to jenkins for use in
       the API call when triggering a build. (templated)
   :type parameters: str, Dict, or List
   :param sleep_time: How long will the operator sleep between each status
       request for the job (min 1, default 10)
   :type sleep_time: int
   :param max_try_before_job_appears: The maximum number of requests to make
       while waiting for the job to appears on jenkins server (default 10)
   :type max_try_before_job_appears: int

   .. attribute:: template_fields
      :annotation: = ['parameters']

      

   .. attribute:: template_ext
      :annotation: = ['.json']

      

   .. attribute:: ui_color
      :annotation: = #f9ec86

      

   
   .. method:: build_job(self, jenkins_server: Jenkins, params: ParamType = '')

      This function makes an API call to Jenkins to trigger a build for 'job_name'
      It returned a dict with 2 keys : body and headers.
      headers contains also a dict-like object which can be queried to get
      the location to poll in the queue.

      :param jenkins_server: The jenkins server where the job should be triggered
      :param params: The parameters block to provide to jenkins API call.
      :return: Dict containing the response body (key body)
          and the headers coming along (headers)



   
   .. method:: poll_job_in_queue(self, location: str, jenkins_server: Jenkins)

      This method poll the jenkins queue until the job is executed.
      When we trigger a job through an API call,
      the job is first put in the queue without having a build number assigned.
      Thus we have to wait the job exit the queue to know its build number.
      To do so, we have to add /api/json (or /api/xml) to the location
      returned by the build_job call and poll this file.
      When a 'executable' block appears in the json, it means the job execution started
      and the field 'number' then contains the build number.

      :param location: Location to poll, returned in the header of the build_job call
      :param jenkins_server: The jenkins server to poll
      :return: The build_number corresponding to the triggered job



   
   .. method:: get_hook(self)

      Instantiate jenkins hook



   
   .. method:: execute(self, context: Mapping[Any, Any])




