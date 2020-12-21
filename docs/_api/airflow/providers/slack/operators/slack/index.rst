:mod:`airflow.providers.slack.operators.slack`
==============================================

.. py:module:: airflow.providers.slack.operators.slack


Module Contents
---------------

.. py:class:: SlackAPIOperator(*, slack_conn_id: Optional[str] = None, token: Optional[str] = None, method: Optional[str] = None, api_params: Optional[Dict] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Base Slack Operator
   The SlackAPIPostOperator is derived from this operator.
   In the future additional Slack API Operators will be derived from this class as well
   Only one of `slack_conn_id` and `token` is required.

   :param slack_conn_id: Slack connection ID which its password is Slack API token. Optional
   :type slack_conn_id: str
   :param token: Slack API token (https://api.slack.com/web). Optional
   :type token: str
   :param method: The Slack API Method to Call (https://api.slack.com/methods). Optional
   :type method: str
   :param api_params: API Method call parameters (https://api.slack.com/methods). Optional
   :type api_params: dict
   :param client_args: Slack Hook parameters. Optional. Check airflow.providers.slack.hooks.SlackHook
   :type api_params: dict

   
   .. method:: construct_api_call_params(self)

      Used by the execute function. Allows templating on the source fields
      of the api_call_params dict before construction

      Override in child classes.
      Each SlackAPIOperator child class is responsible for
      having a construct_api_call_params function
      which sets self.api_call_params with a dict of
      API call parameters (https://api.slack.com/methods)



   
   .. method:: execute(self, **kwargs)

      SlackAPIOperator calls will not fail even if the call is not unsuccessful.
      It should not prevent a DAG from completing in success




.. py:class:: SlackAPIPostOperator(channel: str = '#general', username: str = 'Airflow', text: str = 'No message has been set.\nHere is a cat video instead\nhttps://www.youtube.com/watch?v=J---aiyznGQ', icon_url: str = 'https://raw.githubusercontent.com/apache/airflow/master/airflow/www/static/pin_100.png', attachments: Optional[List] = None, blocks: Optional[List] = None, **kwargs)

   Bases: :class:`airflow.providers.slack.operators.slack.SlackAPIOperator`

   Posts messages to a slack channel
   Examples:

   .. code-block:: python

       slack = SlackAPIPostOperator(
           task_id="post_hello",
           dag=dag,
           token="XXX",
           text="hello there!",
           channel="#random",
       )

   :param channel: channel in which to post message on slack name (#general) or
       ID (C12318391). (templated)
   :type channel: str
   :param username: Username that airflow will be posting to Slack as. (templated)
   :type username: str
   :param text: message to send to slack. (templated)
   :type text: str
   :param icon_url: url to icon used for this message
   :type icon_url: str
   :param attachments: extra formatting details. (templated)
       - see https://api.slack.com/docs/attachments.
   :type attachments: list of hashes
   :param blocks: extra block layouts. (templated)
       - see https://api.slack.com/reference/block-kit/blocks.
   :type blocks: list of hashes

   .. attribute:: template_fields
      :annotation: = ['username', 'text', 'attachments', 'blocks', 'channel']

      

   .. attribute:: ui_color
      :annotation: = #FFBA40

      

   
   .. method:: construct_api_call_params(self)




.. py:class:: SlackAPIFileOperator(channel: str = '#general', initial_comment: str = 'No message has been set!', filename: str = 'default_name.csv', filetype: str = 'csv', content: str = 'default,content,csv,file', **kwargs)

   Bases: :class:`airflow.providers.slack.operators.slack.SlackAPIOperator`

   Send a file to a slack channel
   Examples:

   .. code-block:: python

       slack = SlackAPIFileOperator(
           task_id="slack_file_upload",
           dag=dag,
           slack_conn_id="slack",
           channel="#general",
           initial_comment="Hello World!",
           filename="hello_world.csv",
           filetype="csv",
           content="hello,world,csv,file",
       )

   :param channel: channel in which to sent file on slack name (templated)
   :type channel: str
   :param initial_comment: message to send to slack. (templated)
   :type initial_comment: str
   :param filename: name of the file (templated)
   :type filename: str
   :param filetype: slack filetype. (templated)
       - see https://api.slack.com/types/file
   :type filetype: str
   :param content: file content. (templated)
   :type content: str

   .. attribute:: template_fields
      :annotation: = ['channel', 'initial_comment', 'filename', 'filetype', 'content']

      

   .. attribute:: ui_color
      :annotation: = #44BEDF

      

   
   .. method:: construct_api_call_params(self)




