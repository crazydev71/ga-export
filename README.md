# GA Report Data ETL Tool

## Description
GAReport is a python application which exports Google Anayltics Report Data to Google BigQuery.   

## Prerequisites
Python 3.6, pip, virtual-env (optional)
 
#### GA report api access credential
Follow instructions [here](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py) to get access to Google GA Report API and copy the `client_secret.json` file to config folder.

#### BigQuery access credential
Get BigQuery access credential from [here](https://console.cloud.google.com/apis/credentials/serviceaccountkey) and save to config folder

#### Install dependences
```
pip install -r requirements.txt
```



## Update configuration
In the `export_ga_report.py` file there are several configuration variables that you may need to customize.

`ANALYTICS_REPORT_CLIENT_SECRETS_PATH` : Replace the file path to GA report api client secret if you use different name or path. <br />
`ANALYTICS_REPORT_VIEW_ID` : Get your GA account profile view id [here](https://ga-dev-tools.appspot.com/account-explorer/) and replace with it.


`BIG_QUERY_PROJECT_ID` : Replace with your BigQuery project id. <br />
`BIG_QUERY_SERVICE_ACCOUNT_SECRETS_PATH` : Replace the file path to BigQuery client secret if you use different name or path.
`BIG_QUERY_TABLE_NAME` : Replace the big query table name in this format 'dataset.table'

## Run the script
```
python export_ga_report.py
```


