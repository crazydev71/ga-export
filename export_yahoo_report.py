from google.oauth2 import service_account

from jobs.yahoo_report_job import YahooReportJob
from jobs.exporters.big_query_exporter import BigQueryExporter

# BigQuery API access necessities
BIG_QUERY_PROJECT_ID = 'bigquery-data-infrastructure'
BIG_QUERY_SERVICE_ACCOUNT_SECRETS_PATH = './config/service_account.json'
BIG_QUERY_SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/bigquery'
]
BIG_QUERY_TABLE_NAME = 'yahoo.yahoo_report'


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

    yahoo_export_job = YahooReportJob(exporter=big_query_exporter)

    yahoo_export_job.run()


if __name__ == '__main__':
    main()
