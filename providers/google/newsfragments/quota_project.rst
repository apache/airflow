add-quota-project-support
----------------------

New features
~~~~~~~~~~~~

* Added support for specifying quota/billing projects in Google Cloud providers (#XXXXX)
  
  * This feature allows users to specify a separate project for API quota and billing
  * Can be configured via connection extras or operator parameters
  * Supports all Google Cloud services that use the x-goog-user-project header
  * Backward compatible with existing configurations