| Package    | Version | SHA256                                                           |
|------------|---------|------------------------------------------------------------------|
| google-ads | 20.0.0  | e9031119833a1c9e1fe5bf1bd61d9b36313dcb3b7fc6ab6220f34f03ab1f1a5f |


 * Google Ads were imported with cut "google.ads" prefix:
   `import google.ads.googleads` becomes `import airflow.providers.google_vendor.googleads`
 * Only v12 version of the API was imported, v11 and v13 were removed from the initial commit
