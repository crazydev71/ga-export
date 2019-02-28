import argparse

import httplib2
from apiclient.discovery import build
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from google.oauth2 import service_account

from jobs.ga_export_job import GAExportJob
from jobs.exporters.big_query_exporter import BigQueryExporter

# GA report API access necessities
ANALYTICS_REPORT_DISCOVERY_URI = 'https://analyticsreporting.googleapis.com/$discovery/rest'
ANALYTICS_REPORT_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
ANALYTICS_REPORT_CLIENT_SECRETS_PATH = './config/client_secret.json'
ANALYTICS_REPORT_VIEW_ID = '188060893'

# BigQuery API access necessities
BIG_QUERY_PROJECT_ID = 'bigquery-data-infrastructure'
BIG_QUERY_SERVICE_ACCOUNT_SECRETS_PATH = './config/service_account.json'
BIG_QUERY_SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/bigquery'
]
BIG_QUERY_TABLE_NAME = 'ga.ga_report'


def initialize_analytics_reporting():
    """Initializes the analyticsreporting service object.

    Returns:
    analytics an authorized analyticsreporting service object.
    """
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        parents=[tools.argparser])
    flags = parser.parse_args([])

    # Set up a Flow object to be used if we need to authenticate.
    flow = client.flow_from_clientsecrets(
        filename=ANALYTICS_REPORT_CLIENT_SECRETS_PATH,
        scope=ANALYTICS_REPORT_SCOPES,
        message=tools.message_if_missing(ANALYTICS_REPORT_CLIENT_SECRETS_PATH))

    # Prepare credentials, and authorize HTTP object with them.
    # If the credentials don't exist or are invalid run through the native client
    # flow. The Storage object will ensure that if successful the good
    # credentials will get written back to a file.
    storage = file.Storage('analyticsreporting.dat')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    http = credentials.authorize(http=httplib2.Http())

    # Build the service object.
    analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=ANALYTICS_REPORT_DISCOVERY_URI)

    return analytics


def initialize_service_account_credentials():
    credentials = service_account.Credentials.from_service_account_file(
        BIG_QUERY_SERVICE_ACCOUNT_SECRETS_PATH)
    credentials = credentials.with_scopes(BIG_QUERY_SCOPES)

    return credentials


def main():
    big_query_exporter = BigQueryExporter(
        credentials=initialize_service_account_credentials(),
        project_id=BIG_QUERY_PROJECT_ID,
        table_name=BIG_QUERY_TABLE_NAME)

    ga_export_job = GAExportJob(
        analytics=initialize_analytics_reporting(),
        view_id=ANALYTICS_REPORT_VIEW_ID,
        start_date='7daysAgo',
        end_date='today',
        exporter=big_query_exporter)

    ga_export_job.run()


if __name__ == '__main__':
    main()
