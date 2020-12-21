:mod:`airflow.providers.google.marketing_platform.hooks.analytics`
==================================================================

.. py:module:: airflow.providers.google.marketing_platform.hooks.analytics


Module Contents
---------------

.. py:class:: GoogleAnalyticsHook(api_version: str = 'v3', *args, **kwargs)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Hook for Google Analytics 360.

   
   .. method:: _paginate(self, resource: Resource, list_args: Optional[Dict[str, Any]] = None)



   
   .. method:: get_conn(self)

      Retrieves connection to Google Analytics 360.



   
   .. method:: list_accounts(self)

      Lists accounts list from Google Analytics 360.



   
   .. method:: get_ad_words_link(self, account_id: str, web_property_id: str, web_property_ad_words_link_id: str)

      Returns a web property-Google Ads link to which the user has access.

      :param account_id: ID of the account which the given web property belongs to.
      :type account_id: string
      :param web_property_id: Web property-Google Ads link UA-string.
      :type web_property_id: string
      :param web_property_ad_words_link_id: to retrieve the Google Ads link for.
      :type web_property_ad_words_link_id: string

      :returns: web property-Google Ads
      :rtype: Dict



   
   .. method:: list_ad_words_links(self, account_id: str, web_property_id: str)

      Lists webProperty-Google Ads links for a given web property.

      :param account_id: ID of the account which the given web property belongs to.
      :type account_id: str
      :param web_property_id: Web property UA-string to retrieve the Google Ads links for.
      :type web_property_id: str

      :returns: list of entity Google Ads links.
      :rtype: list



   
   .. method:: upload_data(self, file_location: str, account_id: str, web_property_id: str, custom_data_source_id: str, resumable_upload: bool = False)

      Uploads file to GA via the Data Import API

      :param file_location: The path and name of the file to upload.
      :type file_location: str
      :param account_id: The GA account Id to which the data upload belongs.
      :type account_id: str
      :param web_property_id: UA-string associated with the upload.
      :type web_property_id: str
      :param custom_data_source_id: Custom Data Source Id to which this data import belongs.
      :type custom_data_source_id: str
      :param resumable_upload: flag to upload the file in a resumable fashion, using a
          series of at least two requests.
      :type resumable_upload: bool



   
   .. method:: delete_upload_data(self, account_id: str, web_property_id: str, custom_data_source_id: str, delete_request_body: Dict[str, Any])

      Deletes the uploaded data for a given account/property/dataset

      :param account_id: The GA account Id to which the data upload belongs.
      :type account_id: str
      :param web_property_id: UA-string associated with the upload.
      :type web_property_id: str
      :param custom_data_source_id: Custom Data Source Id to which this data import belongs.
      :type custom_data_source_id: str
      :param delete_request_body: Dict of customDataImportUids to delete.
      :type delete_request_body: dict



   
   .. method:: list_uploads(self, account_id, web_property_id, custom_data_source_id)

      Get list of data upload from GA

      :param account_id: The GA account Id to which the data upload belongs.
      :type account_id: str
      :param web_property_id: UA-string associated with the upload.
      :type web_property_id: str
      :param custom_data_source_id: Custom Data Source Id to which this data import belongs.
      :type custom_data_source_id: str




